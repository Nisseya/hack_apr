"use client";

import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import Link from "next/link";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { toast } from "@/components/ui/toaster";
import {
  GitBranch,
  Globe,
  FlaskConical,
  ArrowRight,
  Server,
  AlertTriangle,
} from "lucide-react";
import { cn } from "@/lib/utils";
import { TEST_BENCHMARKS, type TestBenchmark } from "@/lib/benchmarks";

export default function SubmitPage() {
  const router = useRouter();
  const [repoUrl, setRepoUrl] = useState("");
  const [kind, setKind] = useState<"test" | "global">("test");
  const [benchmark, setBenchmark] = useState<TestBenchmark | "">("");
  const [submitting, setSubmitting] = useState(false);
  const [team, setTeam] = useState<{
    name: string;
    slug: string;
    vmUrl: string | null;
  } | null>(null);
  const [loadingTeam, setLoadingTeam] = useState(true);

  useEffect(() => {
    fetch("/api/me", { cache: "no-store" })
      .then((r) => r.json())
      .then((d) => setTeam(d.team))
      .finally(() => setLoadingTeam(false));
  }, []);

  const submit = async () => {
    setSubmitting(true);
    try {
      const res = await fetch("/api/submissions", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          repoUrl,
          kind,
          benchmarkName: kind === "test" && benchmark ? benchmark : null,
        }),
      });
      const data = await res.json();
      if (!res.ok) {
        toast({
          title: "Submission failed",
          description: data.error ?? "Error",
          variant: "destructive",
        });
        if (data.submissionId) {
          router.push(`/submissions/${data.submissionId}`);
        }
        return;
      }
      router.push(`/submissions/${data.submission.id}?autorun=1`);
    } finally {
      setSubmitting(false);
    }
  };

  if (loadingTeam) {
    return (
      <div className="mx-auto max-w-3xl px-4 py-10 font-mono text-xs uppercase tracking-widest text-muted-foreground">
        Loading...
      </div>
    );
  }

  if (!team) {
    return (
      <div className="mx-auto max-w-3xl px-4 py-20">
        <div className="border border-dashed border-border p-12 text-center space-y-4">
          <div className="font-mono text-xs uppercase tracking-widest text-muted-foreground">
            // no_team_assigned
          </div>
          <h1 className="font-display text-3xl">Join or create a team first</h1>
          <p className="text-sm text-muted-foreground">
            Submissions are made on behalf of a team.
          </p>
          <Button asChild>
            <Link href="/teams">
              Go to teams <ArrowRight className="h-4 w-4" />
            </Link>
          </Button>
        </div>
      </div>
    );
  }

  const vmMissing = !team.vmUrl;
  const canSubmit =
    !!repoUrl && !submitting && (kind === "global" || !vmMissing);

  return (
    <div className="mx-auto max-w-3xl px-4 sm:px-6 lg:px-8 py-10">
      <div className="font-mono text-[10px] uppercase tracking-widest text-muted-foreground">
        // submit_model
      </div>
      <h1 className="font-display text-5xl mt-1">Submit a repo</h1>
      <p className="mt-3 text-sm text-muted-foreground">
        Submitting as{" "}
        <span className="text-primary font-mono">{team.name}</span>.
      </p>

      <Card className="mt-10">
        <CardHeader>
          <CardTitle>Benchmark type</CardTitle>
          <CardDescription>
            Pick <span className="text-primary">test</span> to iterate — you'll
            see each question, generated code, and exact-match, streamed
            against your team's VM. Pick{" "}
            <span className="text-primary">global</span> for the official
            leaderboard run (single aggregate score).
          </CardDescription>
        </CardHeader>
        <CardContent className="grid sm:grid-cols-2 gap-3">
          <KindCard
            icon={FlaskConical}
            title="Test"
            subtitle="visible / detailed"
            desc="Streams against your team's VM. Full question/answer visibility. Pick a specific benchmark below."
            active={kind === "test"}
            onClick={() => setKind("test")}
          />
          <KindCard
            icon={Globe}
            title="Global"
            subtitle="hidden / final"
            desc="Submitted to the official backend. Returns a single aggregated score ranked on the leaderboard."
            active={kind === "global"}
            onClick={() => setKind("global")}
          />
        </CardContent>
      </Card>

      {kind === "test" && (
        <>
          {vmMissing && (
            <div className="mt-6 border border-destructive/40 bg-destructive/5 p-4 flex items-start gap-3">
              <AlertTriangle className="h-4 w-4 text-destructive mt-0.5 flex-shrink-0" />
              <div className="text-sm">
                <div className="font-mono text-xs uppercase tracking-widest text-destructive mb-1">
                  vm_url_missing
                </div>
                <p className="text-muted-foreground">
                  Your team has not configured its VM URL.{" "}
                  <Link
                    href={`/teams/${team.slug}`}
                    className="text-primary underline"
                  >
                    Set it on the team page
                  </Link>{" "}
                  before running a test submission.
                </p>
              </div>
            </div>
          )}

          <Card className="mt-6">
            <CardHeader>
              <CardTitle>Benchmark</CardTitle>
              <CardDescription>
                Choose one of the public benchmarks. Leave on{" "}
                <span className="text-primary">default</span> to run the
                backend's default set.
              </CardDescription>
            </CardHeader>
            <CardContent className="grid gap-2 sm:grid-cols-2">
              <BenchmarkCard
                value=""
                label="Default"
                description="Backend's default benchmark set (15 questions)."
                active={benchmark === ""}
                onClick={() => setBenchmark("")}
              />
              {TEST_BENCHMARKS.map((b) => (
                <BenchmarkCard
                  key={b.value}
                  value={b.value}
                  label={b.label}
                  description={b.description}
                  active={benchmark === b.value}
                  onClick={() => setBenchmark(b.value)}
                />
              ))}
            </CardContent>
          </Card>

          {team.vmUrl && (
            <div className="mt-3 flex items-center gap-2 font-mono text-[10px] uppercase tracking-widest text-muted-foreground">
              <Server className="h-3 w-3" />
              target:{" "}
              <span className="text-primary normal-case">{team.vmUrl}</span>
            </div>
          )}
        </>
      )}

      <Card className="mt-6">
        <CardHeader>
          <CardTitle>Repository</CardTitle>
          <CardDescription>
            A GitHub URL. Your repo must expose a FastAPI <code>main:app</code>{" "}
            with the inference endpoint the benchmark runner expects.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-3">
          <div className="space-y-2">
            <Label htmlFor="repo">GitHub URL</Label>
            <div className="flex items-center gap-2">
              <GitBranch className="h-4 w-4 text-muted-foreground" />
              <Input
                id="repo"
                placeholder="https://github.com/your-team/slm-repo"
                value={repoUrl}
                onChange={(e) => setRepoUrl(e.target.value)}
              />
            </div>
          </div>
        </CardContent>
      </Card>

      <div className="mt-8 flex justify-end gap-3">
        <Button variant="outline" onClick={() => router.push("/submissions")}>
          Cancel
        </Button>
        <Button size="lg" onClick={submit} disabled={!canSubmit}>
          {submitting
            ? "Queuing..."
            : kind === "global"
              ? "Run final benchmark"
              : benchmark
                ? `Run ${benchmark}`
                : "Run test benchmark"}
          <ArrowRight className="h-4 w-4" />
        </Button>
      </div>
    </div>
  );
}

function KindCard({
  icon: Icon,
  title,
  subtitle,
  desc,
  active,
  onClick,
}: {
  icon: React.ComponentType<{ className?: string }>;
  title: string;
  subtitle: string;
  desc: string;
  active: boolean;
  onClick: () => void;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={cn(
        "text-left p-5 border transition-all",
        active
          ? "border-primary bg-primary/5 glow-lime"
          : "border-border hover:border-muted-foreground",
      )}
    >
      <Icon
        className={cn(
          "h-5 w-5 mb-3",
          active ? "text-primary" : "text-muted-foreground",
        )}
      />
      <div className="flex items-baseline gap-2">
        <span className="font-mono text-sm uppercase tracking-widest">
          {title}
        </span>
        <span className="font-mono text-[10px] uppercase tracking-widest text-muted-foreground">
          {subtitle}
        </span>
      </div>
      <p className="mt-2 text-xs leading-relaxed text-muted-foreground">
        {desc}
      </p>
    </button>
  );
}

function BenchmarkCard({
  value,
  label,
  description,
  active,
  onClick,
}: {
  value: string;
  label: string;
  description: string;
  active: boolean;
  onClick: () => void;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={cn(
        "text-left p-3 border transition-all",
        active
          ? "border-primary bg-primary/5"
          : "border-border hover:border-muted-foreground",
      )}
    >
      <div className="flex items-center justify-between gap-2">
        <span className="font-mono text-xs uppercase tracking-widest">
          {label}
        </span>
        {active && (
          <span className="font-mono text-[10px] uppercase tracking-widest text-primary">
            selected
          </span>
        )}
      </div>
      <p className="mt-1 text-[11px] leading-relaxed text-muted-foreground">
        {description}
      </p>
    </button>
  );
}
