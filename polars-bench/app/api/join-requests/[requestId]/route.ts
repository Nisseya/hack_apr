import { NextResponse } from "next/server";
import { db } from "@/db";
import { team, teamMember, joinRequest } from "@/db/schema";
import { getSession } from "@/lib/session";
import { eq } from "drizzle-orm";

export async function POST(
  req: Request,
  { params }: { params: Promise<{ requestId: string }> },
) {
  const session = await getSession();
  if (!session) return NextResponse.json({ error: "unauthorized" }, { status: 401 });

  const { requestId } = await params;
  const body = await req.json().catch(() => ({}));
  const action = body.action as "accept" | "reject";
  if (action !== "accept" && action !== "reject") {
    return NextResponse.json({ error: "invalid action" }, { status: 400 });
  }

  const [jr] = await db
    .select()
    .from(joinRequest)
    .where(eq(joinRequest.id, requestId))
    .limit(1);
  if (!jr) return NextResponse.json({ error: "not found" }, { status: 404 });
  if (jr.status !== "pending")
    return NextResponse.json(
      { error: "request already resolved" },
      { status: 400 },
    );

  const [t] = await db.select().from(team).where(eq(team.id, jr.teamId)).limit(1);
  if (!t) return NextResponse.json({ error: "team missing" }, { status: 404 });
  if (t.ownerId !== session.user.id)
    return NextResponse.json({ error: "forbidden" }, { status: 403 });

  if (action === "accept") {
    // Double-check user not in another team
    const existing = await db
      .select()
      .from(teamMember)
      .where(eq(teamMember.userId, jr.userId))
      .limit(1);

    if (existing.length) {
      await db
        .update(joinRequest)
        .set({ status: "rejected", resolvedAt: new Date() })
        .where(eq(joinRequest.id, jr.id));
      return NextResponse.json(
        { error: "User is already in a team (request auto-rejected)" },
        { status: 400 },
      );
    }

    await db.insert(teamMember).values({
      teamId: jr.teamId,
      userId: jr.userId,
      role: "member",
    });
    await db
      .update(joinRequest)
      .set({ status: "accepted", resolvedAt: new Date() })
      .where(eq(joinRequest.id, jr.id));
  } else {
    await db
      .update(joinRequest)
      .set({ status: "rejected", resolvedAt: new Date() })
      .where(eq(joinRequest.id, jr.id));
  }

  return NextResponse.json({ ok: true });
}
