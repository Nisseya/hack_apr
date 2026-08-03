import { NextResponse } from "next/server";
import { db } from "@/db";
import { team, teamMember, joinRequest } from "@/db/schema";
import { getSession } from "@/lib/session";
import { and, eq } from "drizzle-orm";

export async function POST(
  req: Request,
  { params }: { params: Promise<{ slug: string }> },
) {
  const session = await getSession();
  if (!session) return NextResponse.json({ error: "unauthorized" }, { status: 401 });

  const { slug } = await params;
  const body = await req.json().catch(() => ({}));
  const message = (body.message ?? "").toString().slice(0, 280) || null;

  const [t] = await db.select().from(team).where(eq(team.slug, slug)).limit(1);
  if (!t) return NextResponse.json({ error: "team not found" }, { status: 404 });

  // Already in a team?
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

  // Existing pending request?
  const pending = await db
    .select()
    .from(joinRequest)
    .where(
      and(
        eq(joinRequest.teamId, t.id),
        eq(joinRequest.userId, session.user.id),
        eq(joinRequest.status, "pending"),
      ),
    )
    .limit(1);
  if (pending.length) {
    return NextResponse.json(
      { error: "Request already pending" },
      { status: 400 },
    );
  }

  const [created] = await db
    .insert(joinRequest)
    .values({
      teamId: t.id,
      userId: session.user.id,
      message,
      status: "pending",
    })
    .returning();

  return NextResponse.json({ request: created }, { status: 201 });
}
