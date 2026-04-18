"use client";

import { use, useEffect, useRef, useState } from "react";
import { useSearchParams } from "next/navigation";
import Link from "next/link";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Progress } from "@/components/ui/progress";
import { Badge } from "@/components/ui/badge";
import { SubmissionStatusBadge } from "@/components/status-badge";
import {
  Tabs,
  TabsContent,
  TabsList,
  TabsTrigger,
} from "@/components/ui/tabs";
import {
  formatBytes,
  formatDuration,
  formatPercent,
  relativeTime,
  cn,
} from "@/lib/utils";
import {
  ChevronDown,
  ChevronRight,
  Check,
  X as XIcon,
  AlertTriangle,
  Play,
  GitBranch,
  Cpu,
  Gauge,
  Globe,
  Trophy,
} from "lucide-react";

type ResultRow = {
  id: string;
  questionId: string;
  questionText: string;
  generatedCode: string | null;
  stdout: string | null;
  stderr: string | null;
  success: boolean | null;
  exactMatch: boolean | null;
  generationDurationSeconds: number | null;
  executionDurationSeconds: number | null;
  peakRamMb: number | null;
  peakGpuMb: number | null;
  generatedHash: string | null;
  goldHash: string | null;
  generatedShape: any;
  goldShape: any;
  generatedColumns: any;
  goldColumns: any;
};

type Sub = {
  id: string;
  repoUrl: string;
  kind: "test" | "global";
  benchmarkName: string | null;
  status: "queued" | "running" | "done" | "failed";
  totalQuestions: number;
  completedQuestions: number;
  exactMatchCount: number;
  accuracy: number | null;
  compositeScore: number | null;
  finalScore: number | null;
  avgGenerationSeconds: number | null;
  avgExecutionSeconds: number | null;
  peakRamMb: number | null;
  peakGpuMb: number | null;
  errorMessage: string | null;
  createdAt: string;
  finishedAt: string | null;
  teamName: string;
  teamSlug: string;
  userName: string | null;
};

export default function SubmissionDetail({
  params,
}: {
  params: Promise<{ id: string }>;
}) {
  const { id } = use(params);
  const search = useSearchParams();
  const autorun = search.get("autorun") === "1";

  const [sub, setSub] = useState<Sub | null>(null);
  const [results, setResults] = useState<ResultRow[]>([]);
  const [includeDetails, setIncludeDetails] = useState(true);
  const [logs, setLogs] = useState<
    Array<{ ts: number; event: string; message: string }>
  >([]);
  const [streaming, setStreaming] = useState(false);
  const [currentStep, setCurrentStep] = useState<string>("");
  const readerRef = useRef<ReadableStreamDefaultReader<Uint8Array> | null>(null);
  const hasAutorun = useRef(false);

  const pushLog = (event: string, message: string) =>
    setLogs((prev) => [...prev, { ts: Date.now(), event, message }]);

  const load = async () => {
    const res = await fetch(`/api/submissions/${id}`, { cache: "no-store" });
    if (res.ok) {
      const data = await res.json();
      setSub(data.submission);
      setResults(data.results ?? []);
      setIncludeDetails(data.includeDetails ?? true);
    }
  };

  useEffect(() => {
    load();
  }, [id]);

  const run = async () => {
    if (streaming) return;
    setStreaming(true);
    setLogs([]);
    pushLog("status", "Opening stream...");
    try {
      const response = await fetch(`/api/submissions/${id}/stream`, {
        method: "POST",
      });
      if (!response.ok || !response.body) {
        const err = await response.text().catch(() => "error");
        pushLog("error", err);
        setStreaming(false);
        return;
      }

      const reader = response.body.getReader();
      readerRef.current = reader;
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
          if (chunk.startsWith(":")) continue; // heartbeat
          let event = "message";
          const dataLines: string[] = [];
          for (const line of chunk.split("\n")) {
            if (line.startsWith("event:")) event = line.slice(6).trim();
            else if (line.startsWith("data:"))
              dataLines.push(line.slice(5).trim());
          }
          if (!dataLines.length) continue;
          let data: any;
          try {
            data = JSON.parse(dataLines.join("\n"));
          } catch {
            data = dataLines.join("\n");
          }
          await handleEvent(event, data);
        }
      }
    } catch (e: any) {
      pushLog("error", e?.message ?? String(e));
    } finally {
      setStreaming(false);
      await load();
    }
  };

  const handleEvent = async (event: string, data: any) => {
    if (event === "status") {
      setCurrentStep(data.step ?? "");
      pushLog(
        "status",
        data.message ??
          `${data.step ?? ""} ${data.question_id ? `· ${data.question_id}` : ""}`,
      );
    } else if (event === "question_result") {
      const exec = data.executed_answer ?? {};
      const gen = data.generated_answer ?? {};
      pushLog(
        "question_result",
        `${data.question_id} → ${
          exec.exact_match ? "✓ match" : exec.success ? "✗ mismatch" : "⚠ fail"
        } (gen=${gen.generation_duration_seconds?.toFixed(1)}s)`,
      );
      // Append to local state (so the UI updates before DB re-fetch)
      setResults((prev) => {
        const withoutDup = prev.filter((r) => r.questionId !== data.question_id);
        return [
          ...withoutDup,
          {
            id: `tmp-${data.question_id}`,
            questionId: data.question_id,
            questionText: data.question,
            generatedCode: gen.code ?? null,
            stdout: exec.stdout ?? null,
            stderr: exec.stderr ?? null,
            success: exec.success ?? null,
            exactMatch: exec.exact_match ?? null,
            generationDurationSeconds:
              gen.generation_duration_seconds ?? null,
            executionDurationSeconds:
              exec.execution_duration_seconds ?? null,
            peakRamMb: gen.peak_ram_mb ?? null,
            peakGpuMb: gen.peak_gpu_mb ?? null,
            generatedHash: exec.generated_hash ?? null,
            goldHash: exec.gold_hash ?? null,
            generatedShape: exec.generated_shape ?? null,
            goldShape: exec.gold_shape ?? null,
            generatedColumns: exec.generated_columns ?? null,
            goldColumns: exec.gold_columns ?? null,
          },
        ];
      });
    } else if (event === "done" || event === "final") {
      pushLog(event, JSON.stringify(data));
      await load();
    } else if (event === "error") {
      pushLog("error", data.message ?? "unknown error");
    } else if (event === "submission_started") {
      pushLog("submission_started", `kind=${data.kind}`);
    }
  };

  // Autorun if redirected from submit page
  useEffect(() => {
    if (autorun && sub && !hasAutorun.current && sub.status === "queued") {
      hasAutorun.current = true;
      run();
    }
  }, [autorun, sub]);

  if (!sub)
    return (
      <div className="mx-auto max-w-6xl px-4 py-10 font-mono text-xs uppercase tracking-widest text-muted-foreground">
        Loading...
      </div>
    );

  const progress =
    sub.totalQuestions > 0
      ? (sub.completedQuestions / sub.totalQuestions) * 100
      : 0;

  const sortedResults = [...results].sort((a, b) =>
    a.questionId.localeCompare(b.questionId),
  );

  return (
    <div className="mx-auto max-w-6xl px-4 sm:px-6 lg:px-8 py-10">
      <Link
        href="/submissions"
        className="font-mono text-[10px] uppercase tracking-widest text-muted-foreground hover:text-primary"
      >
        ← submissions
      </Link>

      {/* Header */}
      <div className="mt-4 flex items-start justify-between gap-6">
        <div className="min-w-0 flex-1">
          <div className="flex items-center gap-3 font-mono text-[10px] uppercase tracking-widest text-muted-foreground">
            <span>// run #{sub.id.slice(0, 8)}</span>
            <span>·</span>
            <Link
              href={`/teams/${sub.teamSlug}`}
              className="hover:text-primary"
            >
              {sub.teamName}
            </Link>
            {sub.userName && (
              <>
                <span>·</span>
                <span>by {sub.userName}</span>
              </>
            )}
          </div>
          <h1 className="font-display text-4xl mt-1 flex items-center gap-3 flex-wrap">
            <Badge variant={sub.kind === "global" ? "default" : "secondary"}>
              {sub.kind}
            </Badge>
            {sub.kind === "test" && (
              <Badge variant="outline">
                {sub.benchmarkName ?? "default"}
              </Badge>
            )}
            <SubmissionStatusBadge status={sub.status} />
            <a
              href={sub.repoUrl}
              target="_blank"
              rel="noreferrer"
              className="text-base font-mono normal-case text-muted-foreground hover:text-primary flex items-center gap-1.5 truncate"
            >
              <GitBranch className="h-3.5 w-3.5" />
              {sub.repoUrl.replace(/^https?:\/\/github\.com\//, "")}
            </a>
          </h1>
        </div>

        {(sub.status === "queued" || sub.status === "failed") && (
          <Button onClick={run} disabled={streaming}>
            <Play className="h-4 w-4" />
            {streaming
              ? "Streaming..."
              : sub.status === "failed"
                ? "Retry"
                : "Start run"}
          </Button>
        )}
      </div>

      {/* Progress + KPIs */}
      {sub.kind === "global" ? (
        <div className="mt-8 grid gap-3 sm:grid-cols-2">
          <Kpi
            icon={Trophy}
            label="Final score"
            value={
              sub.finalScore != null
                ? sub.finalScore.toFixed(4)
                : sub.status === "running"
                  ? "running..."
                  : "—"
            }
            hint={
              sub.status === "done"
                ? "official"
                : sub.status === "running"
                  ? "the official backend is computing the score"
                  : sub.status === "failed"
                    ? "run failed"
                    : "pending"
            }
            primary
          />
          <Kpi
            icon={Globe}
            label="Benchmark"
            value="final"
            hint="secret scoring via /submit_final"
          />
        </div>
      ) : (
        <div className="mt-8 grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
          <Kpi
            icon={Trophy}
            label="Composite"
            value={
              sub.compositeScore != null
                ? sub.compositeScore.toFixed(2)
                : "—"
            }
            hint={sub.status === "done" ? "final" : "pending"}
            primary
          />
          <Kpi
            icon={Check}
            label="Accuracy"
            value={formatPercent(sub.accuracy)}
            hint={`${sub.exactMatchCount}/${sub.totalQuestions} exact`}
          />
          <Kpi
            icon={Gauge}
            label="Avg gen"
            value={formatDuration(sub.avgGenerationSeconds)}
            hint={`exec ${formatDuration(sub.avgExecutionSeconds)}`}
          />
          <Kpi
            icon={Cpu}
            label="Peak mem"
            value={formatBytes(
              Math.max(sub.peakRamMb ?? 0, sub.peakGpuMb ?? 0) || null,
            )}
            hint={`ram ${formatBytes(sub.peakRamMb)} · gpu ${formatBytes(sub.peakGpuMb)}`}
          />
        </div>
      )}

      {/* Progress bar — only meaningful for test kind */}
      {sub.kind === "test" && (
        <div className="mt-6">
          <div className="flex items-center justify-between font-mono text-[10px] uppercase tracking-widest text-muted-foreground mb-2">
            <span>
              progress · {sub.completedQuestions} / {sub.totalQuestions}
              {currentStep && (
                <span className="ml-2 text-primary">· {currentStep}</span>
              )}
            </span>
            <span>{progress.toFixed(0)}%</span>
          </div>
          <Progress value={progress} />
        </div>
      )}

      {/* For global: indeterminate running indicator */}
      {sub.kind === "global" && sub.status === "running" && (
        <div className="mt-6 border border-border bg-card p-4 flex items-center gap-3">
          <span className="inline-block h-2 w-2 rounded-full bg-primary animate-pulse-dot" />
          <span className="font-mono text-xs uppercase tracking-widest text-muted-foreground">
            {currentStep || "running final benchmark"}
          </span>
          <span className="ml-auto font-mono text-[10px] uppercase tracking-widest text-muted-foreground">
            may take several minutes
          </span>
        </div>
      )}

      {/* Error */}
      {sub.errorMessage && (
        <div className="mt-6 border border-destructive/40 bg-destructive/10 p-4 text-destructive text-sm">
          <div className="flex items-center gap-2 font-mono text-xs uppercase tracking-widest mb-1">
            <AlertTriangle className="h-3.5 w-3.5" /> error
          </div>
          <pre className="whitespace-pre-wrap font-mono text-xs">
            {sub.errorMessage}
          </pre>
        </div>
      )}

      {/* Tabs */}
      <Tabs
        defaultValue={
          sub.kind === "test" && includeDetails ? "questions" : "summary"
        }
        className="mt-10"
      >
        <TabsList>
          {sub.kind === "test" && includeDetails && (
            <TabsTrigger value="questions">
              Questions · {sortedResults.length}
            </TabsTrigger>
          )}
          <TabsTrigger value="summary">Summary</TabsTrigger>
          <TabsTrigger value="logs">Logs</TabsTrigger>
        </TabsList>

        {sub.kind === "test" && includeDetails && (
          <TabsContent value="questions">
            {sortedResults.length === 0 ? (
              <div className="border border-dashed border-border p-10 text-center font-mono text-xs uppercase tracking-widest text-muted-foreground">
                No question results yet. Start the run.
              </div>
            ) : (
              <div className="space-y-2">
                {sortedResults.map((r, i) => (
                  <QuestionResult key={r.questionId} idx={i + 1} r={r} />
                ))}
              </div>
            )}
          </TabsContent>
        )}

        <TabsContent value="summary">
          <Card>
            <CardHeader>
              <CardTitle>
                {sub.kind === "global" ? "Official score" : "Final summary"}
              </CardTitle>
            </CardHeader>
            <CardContent>
              {sub.kind === "global" ? (
                <dl className="grid grid-cols-2 gap-x-8 gap-y-3 font-mono text-xs">
                  <Row k="Status" v={sub.status} />
                  <Row k="Created" v={relativeTime(sub.createdAt)} />
                  <Row
                    k="Finished"
                    v={sub.finishedAt ? relativeTime(sub.finishedAt) : "—"}
                  />
                  <Row
                    k="Repo"
                    v={sub.repoUrl.replace(/^https?:\/\/github\.com\//, "")}
                  />
                  <Row
                    k="Final score"
                    v={
                      sub.finalScore != null
                        ? sub.finalScore.toFixed(6)
                        : "—"
                    }
                    highlight
                  />
                  {!includeDetails && (
                    <div className="col-span-2 mt-3 pt-3 border-t border-border text-muted-foreground normal-case">
                      This is a global submission. Per-question details are
                      private to the submitting team.
                    </div>
                  )}
                </dl>
              ) : (
                <dl className="grid grid-cols-2 gap-x-8 gap-y-3 font-mono text-xs">
                  <Row k="Status" v={sub.status} />
                  <Row
                    k="Benchmark"
                    v={sub.benchmarkName ?? "default"}
                  />
                  <Row k="Created" v={relativeTime(sub.createdAt)} />
                  <Row
                    k="Finished"
                    v={sub.finishedAt ? relativeTime(sub.finishedAt) : "—"}
                  />
                  <Row
                    k="Repo"
                    v={sub.repoUrl.replace(/^https?:\/\/github\.com\//, "")}
                  />
                  <Row
                    k="Accuracy"
                    v={formatPercent(sub.accuracy)}
                    highlight
                  />
                  <Row
                    k="Composite score"
                    v={
                      sub.compositeScore != null
                        ? sub.compositeScore.toFixed(3)
                        : "—"
                    }
                    highlight
                  />
                  <Row
                    k="Avg generation"
                    v={formatDuration(sub.avgGenerationSeconds)}
                  />
                  <Row
                    k="Avg execution"
                    v={formatDuration(sub.avgExecutionSeconds)}
                  />
                  <Row k="Peak RAM" v={formatBytes(sub.peakRamMb)} />
                  <Row k="Peak GPU" v={formatBytes(sub.peakGpuMb)} />
                </dl>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="logs">
          <Card>
            <CardContent className="p-0">
              <pre className="max-h-[500px] overflow-auto p-4 font-mono text-xs leading-relaxed">
                {logs.length === 0
                  ? "// no events yet"
                  : logs
                      .map(
                        (l) =>
                          `[${new Date(l.ts).toISOString().slice(11, 19)}] ${l.event.padEnd(20)} ${l.message}`,
                      )
                      .join("\n")}
              </pre>
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>
    </div>
  );
}

function Kpi({
  icon: Icon,
  label,
  value,
  hint,
  primary,
}: {
  icon: React.ComponentType<{ className?: string }>;
  label: string;
  value: string;
  hint?: string;
  primary?: boolean;
}) {
  return (
    <Card className={primary ? "border-primary/40" : ""}>
      <CardContent className="p-5">
        <div className="flex items-center gap-2 font-mono text-[10px] uppercase tracking-widest text-muted-foreground">
          <Icon
            className={cn(
              "h-3 w-3",
              primary ? "text-primary" : "text-muted-foreground",
            )}
          />{" "}
          {label}
        </div>
        <div
          className={cn(
            "mt-1 font-display text-3xl",
            primary && "text-primary text-glow",
          )}
        >
          {value}
        </div>
        {hint && (
          <div className="mt-1 font-mono text-[10px] uppercase tracking-widest text-muted-foreground">
            {hint}
          </div>
        )}
      </CardContent>
    </Card>
  );
}

function Row({
  k,
  v,
  highlight,
}: {
  k: string;
  v: string;
  highlight?: boolean;
}) {
  return (
    <>
      <dt className="text-muted-foreground uppercase tracking-widest">{k}</dt>
      <dd className={cn(highlight && "text-primary")}>{v}</dd>
    </>
  );
}

function QuestionResult({ idx, r }: { idx: number; r: ResultRow }) {
  const [open, setOpen] = useState(false);
  const statusIcon =
    r.exactMatch === true ? (
      <Check className="h-3 w-3 text-primary" />
    ) : r.success === false ? (
      <AlertTriangle className="h-3 w-3 text-amber" />
    ) : r.exactMatch === false ? (
      <XIcon className="h-3 w-3 text-destructive" />
    ) : null;

  return (
    <Card
      className={cn(
        "transition-colors",
        r.exactMatch === true && "border-primary/30",
        r.exactMatch === false && "border-destructive/20",
      )}
    >
      <button
        type="button"
        onClick={() => setOpen(!open)}
        className="w-full text-left p-4 flex items-start gap-3 hover:bg-accent/40"
      >
        <div className="font-mono text-[10px] uppercase tracking-widest text-muted-foreground pt-0.5 w-12">
          #{r.questionId}
        </div>
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2">
            {statusIcon}
            <span className="text-sm">{r.questionText}</span>
          </div>
          <div className="mt-1 flex items-center gap-3 font-mono text-[10px] uppercase tracking-widest text-muted-foreground">
            <span>gen {formatDuration(r.generationDurationSeconds)}</span>
            <span>·</span>
            <span>exec {formatDuration(r.executionDurationSeconds)}</span>
            <span>·</span>
            <span>ram {formatBytes(r.peakRamMb)}</span>
            <span>·</span>
            <span>gpu {formatBytes(r.peakGpuMb)}</span>
          </div>
        </div>
        {open ? (
          <ChevronDown className="h-4 w-4 text-muted-foreground" />
        ) : (
          <ChevronRight className="h-4 w-4 text-muted-foreground" />
        )}
      </button>

      {open && (
        <div className="border-t border-border grid lg:grid-cols-2 gap-0">
          <div className="p-4 border-b lg:border-b-0 lg:border-r border-border">
            <div className="font-mono text-[10px] uppercase tracking-widest text-muted-foreground mb-2">
              // generated_code
            </div>
            <pre className="overflow-auto bg-background border border-border p-3 font-mono text-[11px] leading-relaxed max-h-80">
              {r.generatedCode || "—"}
            </pre>
          </div>
          <div className="p-4 space-y-3">
            <div>
              <div className="font-mono text-[10px] uppercase tracking-widest text-muted-foreground mb-2">
                // stdout
              </div>
              <pre className="overflow-auto bg-background border border-border p-3 font-mono text-[11px] leading-relaxed max-h-32">
                {r.stdout || "—"}
              </pre>
            </div>
            {r.stderr && (
              <div>
                <div className="font-mono text-[10px] uppercase tracking-widest text-destructive mb-2">
                  // stderr
                </div>
                <pre className="overflow-auto bg-destructive/5 border border-destructive/30 p-3 font-mono text-[11px] leading-relaxed max-h-24 text-destructive">
                  {r.stderr}
                </pre>
              </div>
            )}
            <div className="grid grid-cols-2 gap-2 font-mono text-[10px] uppercase tracking-widest">
              <div>
                <div className="text-muted-foreground">generated_hash</div>
                <div className="truncate">{r.generatedHash ?? "—"}</div>
              </div>
              <div>
                <div className="text-muted-foreground">gold_hash</div>
                <div className="truncate">{r.goldHash ?? "—"}</div>
              </div>
              <div>
                <div className="text-muted-foreground">generated_shape</div>
                <div>{JSON.stringify(r.generatedShape) ?? "—"}</div>
              </div>
              <div>
                <div className="text-muted-foreground">gold_shape</div>
                <div>{JSON.stringify(r.goldShape) ?? "—"}</div>
              </div>
            </div>
          </div>
        </div>
      )}
    </Card>
  );
}
