import { NextResponse } from "next/server";
import { db } from "@/db";
import { team, teamMember } from "@/db/schema";
import { getSession } from "@/lib/session";
import { eq } from "drizzle-orm";

export async function GET() {
  const session = await getSession();
  if (!session) return NextResponse.json({ user: null, team: null });

  const mem = await db
    .select({
      role: teamMember.role,
      team,
    })
    .from(teamMember)
    .innerJoin(team, eq(team.id, teamMember.teamId))
    .where(eq(teamMember.userId, session.user.id))
    .limit(1);

  return NextResponse.json({
    user: session.user,
    team: mem[0]?.team ?? null,
    role: mem[0]?.role ?? null,
  });
}
