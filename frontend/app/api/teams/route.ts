import { NextResponse } from "next/server";
import { db } from "@/db";
import { team, teamMember } from "@/db/schema";
import { getSession } from "@/lib/session";
import { eq, sql } from "drizzle-orm";
import { slugify } from "@/lib/ids";

export async function GET() {
  const session = await getSession();
  if (!session) return NextResponse.json({ error: "unauthorized" }, { status: 401 });

  // List all teams with member count
  const rows = await db
    .select({
      id: team.id,
      name: team.name,
      slug: team.slug,
      description: team.description,
      ownerId: team.ownerId,
      createdAt: team.createdAt,
      memberCount: sql<number>`cast(count(${teamMember.userId}) as int)`,
    })
    .from(team)
    .leftJoin(teamMember, eq(teamMember.teamId, team.id))
    .groupBy(team.id)
    .orderBy(team.createdAt);

  return NextResponse.json({ teams: rows });
}

export async function POST(req: Request) {
  const session = await getSession();
  if (!session) return NextResponse.json({ error: "unauthorized" }, { status: 401 });

  const body = await req.json().catch(() => ({}));
  const name = (body.name ?? "").toString().trim();
  const description = (body.description ?? "").toString().trim() || null;

  if (name.length < 2 || name.length > 48) {
    return NextResponse.json(
      { error: "Team name must be 2–48 characters" },
      { status: 400 },
    );
  }

  // Ensure user is not already in a team
  const existing = await db
    .select()
    .from(teamMember)
    .where(eq(teamMember.userId, session.user.id))
    .limit(1);
  if (existing.length) {
    return NextResponse.json(
      { error: "You are already in a team" },
      { status: 400 },
    );
  }

  const baseSlug = slugify(name);
  let slug = baseSlug;
  let i = 2;
  while (
    (await db.select().from(team).where(eq(team.slug, slug)).limit(1)).length
  ) {
    slug = `${baseSlug}-${i++}`;
  }

  const [created] = await db
    .insert(team)
    .values({
      name,
      slug,
      description,
      ownerId: session.user.id,
    })
    .returning();

  await db.insert(teamMember).values({
    teamId: created.id,
    userId: session.user.id,
    role: "owner",
  });

  return NextResponse.json({ team: created }, { status: 201 });
}
