import { NextResponse } from "next/server";
import { db } from "@/db";
import { team, teamMember } from "@/db/schema";
import { getSession } from "@/lib/session";
import { and, eq } from "drizzle-orm";

export async function POST(
  _req: Request,
  { params }: { params: Promise<{ slug: string }> },
) {
  const session = await getSession();
  if (!session) return NextResponse.json({ error: "unauthorized" }, { status: 401 });

  const { slug } = await params;
  const [t] = await db.select().from(team).where(eq(team.slug, slug)).limit(1);
  if (!t) return NextResponse.json({ error: "team not found" }, { status: 404 });

  const [mem] = await db
    .select()
    .from(teamMember)
    .where(
      and(eq(teamMember.teamId, t.id), eq(teamMember.userId, session.user.id)),
    )
    .limit(1);
  if (!mem)
    return NextResponse.json({ error: "not a member" }, { status: 400 });

  if (t.ownerId === session.user.id) {
    // Owner leaves → check if sole member. If so, delete team entirely.
    const all = await db
      .select()
      .from(teamMember)
      .where(eq(teamMember.teamId, t.id));
    if (all.length > 1) {
      return NextResponse.json(
        { error: "Transfer ownership before leaving (or remove members first)" },
        { status: 400 },
      );
    }
    await db.delete(team).where(eq(team.id, t.id));
    return NextResponse.json({ ok: true, deleted: true });
  }

  await db
    .delete(teamMember)
    .where(
      and(eq(teamMember.teamId, t.id), eq(teamMember.userId, session.user.id)),
    );
  return NextResponse.json({ ok: true });
}
