import { NextResponse } from "next/server";
import { db } from "@/db";
import { submission, teamMember, team, user } from "@/db/schema";
import { getSession } from "@/lib/session";
import { and, desc, eq, or } from "drizzle-orm";
import { isValidTestBenchmark } from "@/lib/benchmarks";

const DEFAULT_TOTAL_QUESTIONS = 15;

export async function GET(req: Request) {
  const session = await getSession();
  if (!session) return NextResponse.json({ error: "unauthorized" }, { status: 401 });

  const url = new URL(req.url);
  const scope = url.searchParams.get("scope") ?? "team"; // 'team' | 'global'

  if (scope === "team") {
    // Submissions of the user's team (test + global)
    const [myMem] = await db
      .select()
      .from(teamMember)
      .where(eq(teamMember.userId, session.user.id))
      .limit(1);
    if (!myMem) return NextResponse.json({ submissions: [] });

    const rows = await db
      .select({
        s: submission,
        teamName: team.name,
        teamSlug: team.slug,
        userName: user.name,
      })
      .from(submission)
      .innerJoin(team, eq(team.id, submission.teamId))
      .leftJoin(user, eq(user.id, submission.submittedByUserId))
      .where(eq(submission.teamId, myMem.teamId))
      .orderBy(desc(submission.createdAt))
      .limit(200);

    return NextResponse.json({
      submissions: rows.map((r) => ({ ...r.s, teamName: r.teamName, teamSlug: r.teamSlug, userName: r.userName })),
    });
  }

  // Global scope: only show 'global' kind, aggregated per team on a different endpoint
  const rows = await db
    .select({
      s: submission,
      teamName: team.name,
      teamSlug: team.slug,
    })
    .from(submission)
    .innerJoin(team, eq(team.id, submission.teamId))
    .where(eq(submission.kind, "global"))
    .orderBy(desc(submission.createdAt))
    .limit(200);

  return NextResponse.json({
    submissions: rows.map((r) => ({ ...r.s, teamName: r.teamName, teamSlug: r.teamSlug })),
  });
}

export async function POST(req: Request) {
  const session = await getSession();
  if (!session) return NextResponse.json({ error: "unauthorized" }, { status: 401 });

  const body = await req.json().catch(() => ({}));
  const repoUrl = (body.repoUrl ?? "").toString().trim();
  const kind = body.kind as "test" | "global";
  const rawBenchmark = body.benchmarkName;

  if (!repoUrl.match(/^https?:\/\/github\.com\/[\w.-]+\/[\w.-]+/i)) {
    return NextResponse.json(
      { error: "Repo URL must be a valid GitHub URL" },
      { status: 400 },
    );
  }
  if (kind !== "test" && kind !== "global") {
    return NextResponse.json({ error: "Invalid kind" }, { status: 400 });
  }

  // Benchmark name is optional for 'test' (null = backend default).
  // For 'global' it must be absent (the backend picks the final benchmark itself).
  let benchmarkName: string | null = null;
  if (kind === "test") {
    if (rawBenchmark != null && rawBenchmark !== "") {
      if (!isValidTestBenchmark(rawBenchmark)) {
        return NextResponse.json(
          { error: "Invalid benchmark name" },
          { status: 400 },
        );
      }
      benchmarkName = rawBenchmark;
    }
  }

  // Find user's team
  const [mem] = await db
    .select()
    .from(teamMember)
    .where(eq(teamMember.userId, session.user.id))
    .limit(1);
  if (!mem)
    return NextResponse.json(
      { error: "You must belong to a team to submit" },
      { status: 400 },
    );

  // For test: require VM URL configured on the team
  if (kind === "test") {
    const [t] = await db
      .select()
      .from(team)
      .where(eq(team.id, mem.teamId))
      .limit(1);
    if (!t?.vmUrl?.trim()) {
      return NextResponse.json(
        {
          error:
            "Your team has not configured its VM URL. Set it on the team page first.",
        },
        { status: 400 },
      );
    }
  }

  // Prevent concurrent running submissions per team
  const running = await db
    .select()
    .from(submission)
    .where(
      and(
        eq(submission.teamId, mem.teamId),
        or(eq(submission.status, "queued"), eq(submission.status, "running")),
      ),
    )
    .limit(1);
  if (running.length) {
    return NextResponse.json(
      {
        error:
          "Your team already has a running submission. Please wait for it to finish.",
        submissionId: running[0].id,
      },
      { status: 409 },
    );
  }

  const [created] = await db
    .insert(submission)
    .values({
      teamId: mem.teamId,
      submittedByUserId: session.user.id,
      repoUrl,
      kind,
      benchmarkName,
      status: "queued",
      totalQuestions: DEFAULT_TOTAL_QUESTIONS,
    })
    .returning();

  return NextResponse.json({ submission: created }, { status: 201 });
}
