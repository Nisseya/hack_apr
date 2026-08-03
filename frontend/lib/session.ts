import { headers } from "next/headers";
import { auth } from "./auth";
import { db } from "@/db";
import { teamMember, team } from "@/db/schema";
import { eq } from "drizzle-orm";
import { cache } from "react";

export const getSession = cache(async () => {
  const session = await auth.api.getSession({
    headers: await headers(),
  });
  return session;
});

export const getSessionWithTeam = cache(async () => {
  const session = await getSession();
  if (!session) return null;

  const membership = await db
    .select({
      teamId: teamMember.teamId,
      role: teamMember.role,
      team,
    })
    .from(teamMember)
    .innerJoin(team, eq(team.id, teamMember.teamId))
    .where(eq(teamMember.userId, session.user.id))
    .limit(1);

  return {
    session,
    user: session.user,
    membership: membership[0] ?? null,
  };
});
