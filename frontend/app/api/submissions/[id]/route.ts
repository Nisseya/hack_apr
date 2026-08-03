import { NextResponse } from "next/server";
import { db } from "@/db";
import { submission, teamMember, team, benchmarkResult, user } from "@/db/schema";
import { getSession } from "@/lib/session";
import { and, eq } from "drizzle-orm";

export async function GET(
  _req: Request,
  { params }: { params: Promise<{ id: string }> },
) {
  const session = await getSession();
  if (!session) return NextResponse.json({ error: "unauthorized" }, { status: 401 });

  const { id } = await params;

  const [row] = await db
    .select({
      s: submission,
      teamName: team.name,
      teamSlug: team.slug,
      userName: user.name,
      userImage: user.image,
    })
    .from(submission)
    .innerJoin(team, eq(team.id, submission.teamId))
    .leftJoin(user, eq(user.id, submission.submittedByUserId))
    .where(eq(submission.id, id))
    .limit(1);

  if (!row) return NextResponse.json({ error: "not found" }, { status: 404 });

  // Visibility: the user must be a member of the submission's team,
  // OR the submission is 'global' (leaderboard shows aggregates only,
  // so we still return submission header for public pages but NO per-question details)
  const [mem] = await db
    .select()
    .from(teamMember)
    .where(
      and(
        eq(teamMember.teamId, row.s.teamId),
        eq(teamMember.userId, session.user.id),
      ),
    )
    .limit(1);

  const isTeamMember = !!mem;
  const isGlobal = row.s.kind === "global";

  if (!isTeamMember && !isGlobal) {
    return NextResponse.json({ error: "forbidden" }, { status: 403 });
  }

  // For global submissions viewed by non-team-members: no detailed results
  const includeDetails = isTeamMember;

  const results = includeDetails
    ? await db
        .select()
        .from(benchmarkResult)
        .where(eq(benchmarkResult.submissionId, id))
    : [];

  return NextResponse.json({
    submission: {
      ...row.s,
      teamName: row.teamName,
      teamSlug: row.teamSlug,
      userName: row.userName,
      userImage: row.userImage,
    },
    results,
    includeDetails,
  });
}
