import { db } from "@/db";
import {
  submission,
  teamMember,
  benchmarkResult,
  team,
} from "@/db/schema";
import { getSession } from "@/lib/session";
import { and, eq } from "drizzle-orm";
import { computeCompositeScore } from "@/lib/scoring";

export const runtime = "nodejs";
export const maxDuration = 3600;
export const dynamic = "force-dynamic";

const DEFAULT_TEST_URL =
  process.env.BACKEND_TEST_URL || "http://host.docker.internal:8000";
const GLOBAL_SECRET = process.env.BACKEND_GLOBAL_SECRET || "";

function normalizeBackendUrl(url: string): string {
  return url.replace(/\/+$/, "");
}

async function* parseSSE(
  reader: ReadableStreamDefaultReader<Uint8Array>,
): AsyncGenerator<{ event: string; data: any }, void, void> {
  const decoder = new TextDecoder();
  let buffer = "";
  while (true) {
    const { value, done } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });

    let sep: number;
    while ((sep = buffer.indexOf("\n\n")) !== -1) {
      const chunk = buffer.slice(0, sep);
      buffer = buffer.slice(sep + 2);

      let event = "message";
      const dataLines: string[] = [];
      for (const line of chunk.split("\n")) {
        if (line.startsWith("event:")) event = line.slice(6).trim();
        else if (line.startsWith("data:")) dataLines.push(line.slice(5).trim());
      }
      if (!dataLines.length) continue;
      const raw = dataLines.join("\n");
      let data: any = raw;
      try {
        data = JSON.parse(raw);
      } catch {
        /* keep raw */
      }
      yield { event, data };
    }
  }
}

function sseFormat(event: string, data: unknown): string {
  return `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
}

function interpretBackendError(status: number, body: string): string {
  if (status === 409) {
    return "The benchmark runner is currently busy with another submission. Please wait a moment and retry.";
  }
  if (status === 403) {
    return "Server misconfiguration: the global submission secret is invalid. Contact the organizers.";
  }
  if (status === 400) {
    try {
      const parsed = JSON.parse(body);
      return `Backend rejected the request: ${parsed.detail ?? body}`;
    } catch {
      return `Backend rejected the request (400): ${body.slice(0, 400)}`;
    }
  }
  if (status === 502 || status === 503 || status === 504) {
    return `Backend is unreachable or overloaded (${status}). Check your team's VM URL.`;
  }
  try {
    const parsed = JSON.parse(body);
    return typeof parsed.detail === "string"
      ? parsed.detail
      : body.slice(0, 500);
  } catch {
    return body.slice(0, 500) || `Backend returned ${status}`;
  }
}

export async function POST(
  _req: Request,
  { params }: { params: Promise<{ id: string }> },
) {
  const session = await getSession();
  if (!session)
    return new Response(JSON.stringify({ error: "unauthorized" }), {
      status: 401,
    });

  const { id: submissionId } = await params;

  const [row] = await db
    .select({ s: submission, t: team })
    .from(submission)
    .innerJoin(team, eq(team.id, submission.teamId))
    .where(eq(submission.id, submissionId))
    .limit(1);

  if (!row)
    return new Response(JSON.stringify({ error: "not found" }), {
      status: 404,
    });

  const sub = row.s;
  const teamRow = row.t;

  const [mem] = await db
    .select()
    .from(teamMember)
    .where(
      and(
        eq(teamMember.teamId, sub.teamId),
        eq(teamMember.userId, session.user.id),
      ),
    )
    .limit(1);
  if (!mem)
    return new Response(JSON.stringify({ error: "forbidden" }), {
      status: 403,
    });

  if (sub.status === "done") {
    return new Response(
      JSON.stringify({ error: "submission already completed" }),
      { status: 409 },
    );
  }
  if (sub.status === "running") {
    return new Response(
      JSON.stringify({ error: "submission already running" }),
      { status: 409 },
    );
  }

  // Both test and global submissions target the team's VM now.
  const vm = teamRow.vmUrl?.trim();
  if (!vm) {
    return new Response(
      JSON.stringify({
        error:
          "Your team has not configured its VM URL. Ask the owner to set it on the team page before submitting.",
      }),
      { status: 400 },
    );
  }

  if (sub.kind === "global" && !GLOBAL_SECRET) {
    return new Response(
      JSON.stringify({
        error:
          "Global backend secret is not configured on the server. Contact the organizers.",
      }),
      { status: 500 },
    );
  }

  // Reset for retry
  await db
    .delete(benchmarkResult)
    .where(eq(benchmarkResult.submissionId, submissionId));
  await db
    .update(submission)
    .set({
      status: "running",
      startedAt: new Date(),
      completedQuestions: 0,
      exactMatchCount: 0,
      avgGenerationSeconds: null,
      avgExecutionSeconds: null,
      peakRamMb: null,
      peakGpuMb: null,
      accuracy: null,
      compositeScore: null,
      finalScore: null,
      errorMessage: null,
      finishedAt: null,
    })
    .where(eq(submission.id, submissionId));

  const encoder = new TextEncoder();

  const stream = new ReadableStream({
    async start(controller) {
      const send = (event: string, data: unknown) => {
        try {
          controller.enqueue(encoder.encode(sseFormat(event, data)));
        } catch {
          /* client gone */
        }
      };

      const heartbeat = setInterval(() => {
        try {
          controller.enqueue(encoder.encode(": heartbeat\n\n"));
        } catch {
          /* ignore */
        }
      }, 15000);

      send("submission_started", {
        submissionId,
        kind: sub.kind,
        benchmark: sub.benchmarkName ?? null,
      });

      try {
        if (sub.kind === "test") {
          await runTestBenchmark({ sub, teamRow, send });
        } else {
          await runGlobalBenchmark({ sub, teamRow, send });
        }
      } catch (e: any) {
        const msg = e?.message ?? String(e);
        await db
          .update(submission)
          .set({
            status: "failed",
            finishedAt: new Date(),
            errorMessage: msg.slice(0, 2000),
          })
          .where(eq(submission.id, submissionId));
        send("error", { message: msg });
      } finally {
        clearInterval(heartbeat);
        try {
          controller.close();
        } catch {
          /* ignore */
        }
      }
    },
  });

  return new Response(stream, {
    headers: {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache, no-transform",
      Connection: "keep-alive",
      "X-Accel-Buffering": "no",
    },
  });
}

/* ------------------------------------------------------------------ */
/* TEST benchmark: SSE stream against the team's VM URL               */
/* ------------------------------------------------------------------ */

async function runTestBenchmark({
  sub,
  teamRow,
  send,
}: {
  sub: typeof submission.$inferSelect;
  teamRow: typeof team.$inferSelect;
  send: (event: string, data: unknown) => void;
}) {
  const backend = normalizeBackendUrl(teamRow.vmUrl || DEFAULT_TEST_URL);
  const target = `${backend}/run-repo-stream`;

  send("status", {
    step: "connecting",
    message: `Connecting to team VM at ${backend}`,
    benchmark: sub.benchmarkName ?? null,
  });

  let backendResponse: Response;
  try {
    backendResponse = await fetch(target, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        repo_url: sub.repoUrl,
        benchmark: sub.benchmarkName ?? null,
      }),
    });
  } catch (e: any) {
    throw new Error(
      `Could not reach your team's VM at ${backend}. Is the FastAPI server running? (${e?.message ?? e})`,
    );
  }

  if (!backendResponse.ok || !backendResponse.body) {
    const body = await backendResponse.text().catch(() => "");
    throw new Error(interpretBackendError(backendResponse.status, body));
  }

  const reader = backendResponse.body.getReader();
  const aggregates = {
    genDurations: [] as number[],
    execDurations: [] as number[],
    peakRam: 0,
    peakGpu: 0,
    exactMatches: 0,
    completed: 0,
  };

  for await (const { event, data } of parseSSE(reader)) {
    if (event === "status") {
      send("status", data);
    } else if (event === "question_result") {
      const gen = data.generated_answer ?? {};
      const exec = data.executed_answer ?? {};

      await db
        .insert(benchmarkResult)
        .values({
          submissionId: sub.id,
          questionId: data.question_id,
          questionText: data.question,
          generatedCode: gen.code ?? null,
          stdout: exec.stdout ?? null,
          stderr: exec.stderr ?? null,
          success: exec.success ?? null,
          exactMatch: exec.exact_match ?? null,
          generationDurationSeconds: gen.generation_duration_seconds ?? null,
          executionDurationSeconds: exec.execution_duration_seconds ?? null,
          peakRamMb: gen.peak_ram_mb ?? null,
          peakGpuMb: gen.peak_gpu_mb ?? null,
          generatedHash: exec.generated_hash ?? null,
          goldHash: exec.gold_hash ?? null,
          generatedShape: exec.generated_shape ?? null,
          goldShape: exec.gold_shape ?? null,
          generatedColumns: exec.generated_columns ?? null,
          goldColumns: exec.gold_columns ?? null,
        })
        .onConflictDoNothing();

      if (typeof gen.generation_duration_seconds === "number")
        aggregates.genDurations.push(gen.generation_duration_seconds);
      if (typeof exec.execution_duration_seconds === "number")
        aggregates.execDurations.push(exec.execution_duration_seconds);
      if (typeof gen.peak_ram_mb === "number")
        aggregates.peakRam = Math.max(aggregates.peakRam, gen.peak_ram_mb);
      if (typeof gen.peak_gpu_mb === "number")
        aggregates.peakGpu = Math.max(aggregates.peakGpu, gen.peak_gpu_mb);
      if (exec.exact_match === true) aggregates.exactMatches += 1;
      aggregates.completed += 1;

      const avgGen =
        aggregates.genDurations.reduce((a, b) => a + b, 0) /
        Math.max(1, aggregates.genDurations.length);
      const avgExec =
        aggregates.execDurations.reduce((a, b) => a + b, 0) /
        Math.max(1, aggregates.execDurations.length);

      await db
        .update(submission)
        .set({
          totalQuestions: Math.max(sub.totalQuestions, aggregates.completed),
          completedQuestions: aggregates.completed,
          exactMatchCount: aggregates.exactMatches,
          avgGenerationSeconds: avgGen,
          avgExecutionSeconds: avgExec,
          peakRamMb: aggregates.peakRam,
          peakGpuMb: aggregates.peakGpu,
        })
        .where(eq(submission.id, sub.id));

      send("question_result", data);
    } else if (event === "done") {
      send("done", data);
      const total = Math.max(aggregates.completed, 1);
      const accuracy = aggregates.exactMatches / total;
      const avgGen =
        aggregates.genDurations.reduce((a, b) => a + b, 0) /
        Math.max(1, aggregates.genDurations.length);
      const avgExec =
        aggregates.execDurations.reduce((a, b) => a + b, 0) /
        Math.max(1, aggregates.execDurations.length);
      const composite = computeCompositeScore({
        accuracy,
        avgGenerationSeconds: avgGen || null,
        avgExecutionSeconds: avgExec || null,
        peakRamMb: aggregates.peakRam || null,
        peakGpuMb: aggregates.peakGpu || null,
      });
      await db
        .update(submission)
        .set({
          status: "done",
          finishedAt: new Date(),
          accuracy,
          compositeScore: composite,
          totalQuestions: total,
        })
        .where(eq(submission.id, sub.id));
      send("final", {
        accuracy,
        compositeScore: composite,
        exactMatches: aggregates.exactMatches,
        total,
      });
    } else if (event === "error") {
      const msg =
        typeof data?.message === "string" ? data.message : "Backend error";
      throw new Error(msg);
    }
  }
}

/* ------------------------------------------------------------------ */
/* GLOBAL benchmark: POST /submit_final (JSON, single score)          */
/* ------------------------------------------------------------------ */

async function runGlobalBenchmark({
  sub,
  teamRow,
  send,
}: {
  sub: typeof submission.$inferSelect;
  teamRow: typeof team.$inferSelect;
  send: (event: string, data: unknown) => void;
}) {
  const backend = normalizeBackendUrl(teamRow.vmUrl || DEFAULT_TEST_URL);
  const target = `${backend}/submit_final`;

  send("status", {
    step: "connecting",
    message: `Submitting to the official final benchmark at ${backend}`,
  });

  let response: Response;
  try {
    response = await fetch(target, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        repo_url: sub.repoUrl,
        secret: GLOBAL_SECRET,
      }),
    });
  } catch (e: any) {
    throw new Error(
      `Could not reach the global benchmark server: ${e?.message ?? e}`,
    );
  }

  if (!response.ok) {
    const body = await response.text().catch(() => "");
    throw new Error(interpretBackendError(response.status, body));
  }

  let payload: { score?: number };
  try {
    payload = await response.json();
  } catch {
    throw new Error("Global backend returned an invalid response");
  }

  const score = typeof payload.score === "number" ? payload.score : null;
  if (score === null) {
    throw new Error("Global backend did not return a numeric score");
  }

  send("status", {
    step: "completed",
    message: "Final benchmark completed",
  });

  await db
    .update(submission)
    .set({
      status: "done",
      finishedAt: new Date(),
      finalScore: score,
    })
    .where(eq(submission.id, sub.id));

  send("final", { finalScore: score });
  send("done", { success: true, finalScore: score });
}
