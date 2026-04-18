import { NextResponse } from "next/server";
import { db } from "@/db";
import { submission, team } from "@/db/schema";
import { and, desc, eq, isNotNull } from "drizzle-orm";

export const dynamic = "force-dynamic";

export async function GET() {
  // Leaderboard shows best 'global' 'done' submission per team ranked by
  // the finalScore returned by the backend's /submit_final endpoint.
  const rows = await db
    .select({
      s: submission,
      teamId: team.id,
      teamName: team.name,
      teamSlug: team.slug,
    })
    .from(submission)
    .innerJoin(team, eq(team.id, submission.teamId))
    .where(
      and(
        eq(submission.kind, "global"),
        eq(submission.status, "done"),
        isNotNull(submission.finalScore),
      ),
    )
    .orderBy(desc(submission.finalScore));

  // Best-of per team (highest finalScore)
  const bestByTeam = new Map<
    string,
    (typeof rows)[number] & { bestSubmissionId: string }
  >();
  for (const row of rows) {
    const existing = bestByTeam.get(row.teamId);
    if (
      !existing ||
      (row.s.finalScore ?? 0) > (existing.s.finalScore ?? 0)
    ) {
      bestByTeam.set(row.teamId, { ...row, bestSubmissionId: row.s.id });
    }
  }

  const leaderboard = Array.from(bestByTeam.values())
    .sort((a, b) => (b.s.finalScore ?? 0) - (a.s.finalScore ?? 0))
    .map((row, i) => ({
      rank: i + 1,
      teamId: row.teamId,
      teamName: row.teamName,
      teamSlug: row.teamSlug,
      bestSubmissionId: row.bestSubmissionId,
      finalScore: row.s.finalScore,
      finishedAt: row.s.finishedAt,
    }));

  return NextResponse.json({ leaderboard });
}
