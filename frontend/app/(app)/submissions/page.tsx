"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { SubmissionStatusBadge } from "@/components/status-badge";
import { Badge } from "@/components/ui/badge";
import { formatDuration, formatPercent, relativeTime } from "@/lib/utils";
import { Plus, GitBranch } from "lucide-react";

type SubmissionRow = {
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
  createdAt: string;
  finishedAt: string | null;
  teamName: string;
  userName: string | null;
};

export default function SubmissionsPage() {
  const [subs, setSubs] = useState<SubmissionRow[]>([]);
  const [loading, setLoading] = useState(true);

  const load = () =>
    fetch("/api/submissions?scope=team", { cache: "no-store" })
      .then((r) => r.json())
      .then((d) => setSubs(d.submissions ?? []))
      .finally(() => setLoading(false));

  useEffect(() => {
    load();
    const i = setInterval(load, 5000);
    return () => clearInterval(i);
  }, []);

  return (
    <div className="mx-auto max-w-7xl px-4 sm:px-6 lg:px-8 py-10">
      <div className="flex items-end justify-between mb-10">
        <div>
          <div className="font-mono text-[10px] uppercase tracking-widest text-muted-foreground">
            // team_activity
          </div>
          <h1 className="font-display text-5xl mt-1">Submissions</h1>
        </div>
        <Button asChild>
          <Link href="/submit">
            <Plus className="h-4 w-4" /> New submission
          </Link>
        </Button>
      </div>

      {loading ? (
        <div className="font-mono text-xs uppercase tracking-widest text-muted-foreground">
          Loading...
        </div>
      ) : subs.length === 0 ? (
        <div className="border border-dashed border-border p-12 text-center font-mono text-sm text-muted-foreground">
          No submissions yet. Fire the first run.
        </div>
      ) : (
        <Card>
          <CardContent className="p-0">
            <table className="w-full">
              <thead className="border-b border-border">
                <tr className="font-mono text-[10px] uppercase tracking-widest text-muted-foreground text-left">
                  <th className="p-4">Repo</th>
                  <th className="p-4">Kind</th>
                  <th className="p-4">Status</th>
                  <th className="p-4 hidden md:table-cell">Progress</th>
                  <th className="p-4 hidden md:table-cell">Accuracy</th>
                  <th className="p-4 hidden lg:table-cell">Score</th>
                  <th className="p-4">When</th>
                  <th className="p-4"></th>
                </tr>
              </thead>
              <tbody>
                {subs.map((s) => (
                  <tr
                    key={s.id}
                    className="border-b border-border last:border-b-0 hover:bg-accent/40 transition-colors"
                  >
                    <td className="p-4">
                      <div className="flex items-center gap-2 font-mono text-xs max-w-[280px] truncate">
                        <GitBranch className="h-3 w-3 text-muted-foreground flex-shrink-0" />
                        <span className="truncate">
                          {s.repoUrl.replace(/^https?:\/\/github\.com\//, "")}
                        </span>
                      </div>
                    </td>
                    <td className="p-4">
                      <div className="flex flex-col gap-1">
                        <Badge
                          variant={s.kind === "global" ? "default" : "secondary"}
                        >
                          {s.kind}
                        </Badge>
                        {s.kind === "test" && (
                          <span className="font-mono text-[10px] uppercase tracking-widest text-muted-foreground">
                            {s.benchmarkName ?? "default"}
                          </span>
                        )}
                      </div>
                    </td>
                    <td className="p-4">
                      <SubmissionStatusBadge status={s.status} />
                    </td>
                    <td className="p-4 hidden md:table-cell font-mono text-xs">
                      {s.kind === "global"
                        ? "—"
                        : `${s.completedQuestions}/${s.totalQuestions}`}
                    </td>
                    <td className="p-4 hidden md:table-cell font-mono text-xs">
                      {s.kind === "global" ? "—" : formatPercent(s.accuracy)}
                    </td>
                    <td className="p-4 hidden lg:table-cell font-mono text-xs text-primary">
                      {s.kind === "global"
                        ? s.finalScore != null
                          ? s.finalScore.toFixed(3)
                          : "—"
                        : s.compositeScore != null
                          ? s.compositeScore.toFixed(2)
                          : "—"}
                    </td>
                    <td className="p-4 font-mono text-xs text-muted-foreground whitespace-nowrap">
                      {relativeTime(s.createdAt)}
                    </td>
                    <td className="p-4">
                      <Button variant="ghost" size="sm" asChild>
                        <Link href={`/submissions/${s.id}`}>Open →</Link>
                      </Button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </CardContent>
        </Card>
      )}
    </div>
  );
}
