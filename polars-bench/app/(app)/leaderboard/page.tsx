"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { Card, CardContent } from "@/components/ui/card";
import { relativeTime, cn } from "@/lib/utils";
import { Trophy } from "lucide-react";

type Entry = {
  rank: number;
  teamId: string;
  teamName: string;
  teamSlug: string;
  bestSubmissionId: string;
  finalScore: number | null;
  finishedAt: string | null;
};

export default function LeaderboardPage() {
  const [entries, setEntries] = useState<Entry[]>([]);
  const [loading, setLoading] = useState(true);

  const load = () =>
    fetch("/api/leaderboard", { cache: "no-store" })
      .then((r) => r.json())
      .then((d) => setEntries(d.leaderboard ?? []))
      .finally(() => setLoading(false));

  useEffect(() => {
    load();
    const i = setInterval(load, 8000);
    return () => clearInterval(i);
  }, []);

  const top = entries[0];

  return (
    <div className="mx-auto max-w-7xl px-4 sm:px-6 lg:px-8 py-10">
      <div className="flex items-end justify-between mb-10">
        <div>
          <div className="font-mono text-[10px] uppercase tracking-widest text-muted-foreground">
            // global_rankings
          </div>
          <h1 className="font-display text-5xl mt-1">
            Leader<span className="italic text-primary">board</span>
          </h1>
          <p className="mt-3 text-sm text-muted-foreground max-w-xl">
            Best global submission per team, scored by the official{" "}
            <code className="font-mono text-xs">/submit_final</code> endpoint.
            Higher is better. Live-refreshes every 8 seconds.
          </p>
        </div>
      </div>

      {loading ? (
        <div className="font-mono text-xs uppercase tracking-widest text-muted-foreground">
          Loading...
        </div>
      ) : entries.length === 0 ? (
        <div className="border border-dashed border-border p-16 text-center">
          <Trophy className="h-10 w-10 mx-auto text-muted-foreground mb-4" />
          <div className="font-mono text-xs uppercase tracking-widest text-muted-foreground">
            No global submissions yet — be the first.
          </div>
        </div>
      ) : (
        <>
          {top && (
            <Card className="relative mb-6 border-primary/40 overflow-hidden">
              <div className="absolute inset-0 bg-gradient-to-r from-primary/10 via-transparent to-transparent pointer-events-none" />
              <div className="scan-line absolute inset-0 pointer-events-none" />
              <CardContent className="relative p-8 flex items-center gap-8">
                <div className="font-display text-[12rem] leading-none text-primary/80 text-glow">
                  1
                </div>
                <div className="flex-1">
                  <div className="font-mono text-[10px] uppercase tracking-widest text-primary">
                    // current_leader
                  </div>
                  <Link
                    href={`/teams/${top.teamSlug}`}
                    className="font-display text-5xl hover:text-primary"
                  >
                    {top.teamName}
                  </Link>
                  <div className="mt-6">
                    <div className="text-[10px] uppercase tracking-widest text-muted-foreground font-mono">
                      final score
                    </div>
                    <div className="font-display text-6xl text-primary text-glow mt-1">
                      {top.finalScore?.toFixed(4) ?? "—"}
                    </div>
                    {top.finishedAt && (
                      <div className="mt-2 font-mono text-[10px] uppercase tracking-widest text-muted-foreground">
                        submitted {relativeTime(top.finishedAt)}
                      </div>
                    )}
                  </div>
                </div>
              </CardContent>
            </Card>
          )}

          <Card>
            <CardContent className="p-0">
              <table className="w-full">
                <thead>
                  <tr className="font-mono text-[10px] uppercase tracking-widest text-muted-foreground text-left border-b border-border">
                    <th className="p-4 w-16">#</th>
                    <th className="p-4">Team</th>
                    <th className="p-4">Final score</th>
                    <th className="p-4 hidden md:table-cell">Finished</th>
                  </tr>
                </thead>
                <tbody>
                  {entries.map((e) => (
                    <tr
                      key={e.teamId}
                      className={cn(
                        "border-b border-border last:border-b-0 hover:bg-accent/40 transition-colors",
                        e.rank === 1 && "bg-primary/[0.03]",
                      )}
                    >
                      <td className="p-4 font-display text-2xl">
                        <span className={e.rank <= 3 ? "text-primary" : ""}>
                          {e.rank}
                        </span>
                      </td>
                      <td className="p-4">
                        <Link
                          href={`/teams/${e.teamSlug}`}
                          className="font-mono text-sm hover:text-primary"
                        >
                          {e.teamName}
                        </Link>
                      </td>
                      <td className="p-4 font-mono text-lg text-primary">
                        {e.finalScore?.toFixed(4) ?? "—"}
                      </td>
                      <td className="p-4 hidden md:table-cell font-mono text-[10px] uppercase tracking-widest text-muted-foreground">
                        {relativeTime(e.finishedAt)}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </CardContent>
          </Card>
        </>
      )}
    </div>
  );
}
