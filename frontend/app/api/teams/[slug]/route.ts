import { NextResponse } from "next/server";
import { db } from "@/db";
import { team, teamMember, user, joinRequest } from "@/db/schema";
import { getSession } from "@/lib/session";
import { and, eq } from "drizzle-orm";

export async function GET(
  _req: Request,
  { params }: { params: Promise<{ slug: string }> },
) {
  const session = await getSession();
  if (!session) return NextResponse.json({ error: "unauthorized" }, { status: 401 });

  const { slug } = await params;

  const [t] = await db.select().from(team).where(eq(team.slug, slug)).limit(1);
  if (!t) return NextResponse.json({ error: "not found" }, { status: 404 });

  const members = await db
    .select({
      userId: teamMember.userId,
      role: teamMember.role,
      joinedAt: teamMember.joinedAt,
      name: user.name,
      email: user.email,
      image: user.image,
    })
    .from(teamMember)
    .innerJoin(user, eq(user.id, teamMember.userId))
    .where(eq(teamMember.teamId, t.id));

  // Only show join requests if current user is the owner
  let requests: unknown[] = [];
  if (t.ownerId === session.user.id) {
    requests = await db
      .select({
        id: joinRequest.id,
        status: joinRequest.status,
        message: joinRequest.message,
        createdAt: joinRequest.createdAt,
        userId: joinRequest.userId,
        userName: user.name,
        userImage: user.image,
        userEmail: user.email,
      })
      .from(joinRequest)
      .innerJoin(user, eq(user.id, joinRequest.userId))
      .where(and(eq(joinRequest.teamId, t.id), eq(joinRequest.status, "pending")));
  }

  const myMembership = members.find((m) => m.userId === session.user.id);
  const myRequest = await db
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

  // VM URL is only returned to members (and owner)
  const isMember = !!myMembership;
  const publicTeam = {
    id: t.id,
    name: t.name,
    slug: t.slug,
    description: t.description,
    ownerId: t.ownerId,
    createdAt: t.createdAt,
    // only members see the VM URL
    vmUrl: isMember ? t.vmUrl : null,
    // non-members just get a boolean
    vmUrlConfigured: !!t.vmUrl,
  };

  return NextResponse.json({
    team: publicTeam,
    members,
    joinRequests: requests,
    currentUserRole: myMembership?.role ?? null,
    hasPendingRequest: myRequest.length > 0,
  });
}

export async function PATCH(
  req: Request,
  { params }: { params: Promise<{ slug: string }> },
) {
  const session = await getSession();
  if (!session) return NextResponse.json({ error: "unauthorized" }, { status: 401 });

  const { slug } = await params;
  const [t] = await db.select().from(team).where(eq(team.slug, slug)).limit(1);
  if (!t) return NextResponse.json({ error: "not found" }, { status: 404 });
  if (t.ownerId !== session.user.id)
    return NextResponse.json(
      { error: "Only the team owner can edit team settings" },
      { status: 403 },
    );

  const body = await req.json().catch(() => ({}));
  const update: Partial<typeof team.$inferInsert> = {};

  if ("description" in body) {
    const desc =
      typeof body.description === "string"
        ? body.description.slice(0, 280)
        : null;
    update.description = desc || null;
  }

  if ("vmUrl" in body) {
    if (body.vmUrl === null || body.vmUrl === "") {
      update.vmUrl = null;
    } else if (typeof body.vmUrl === "string") {
      const trimmed = body.vmUrl.trim();
      if (!/^https?:\/\/[^\s]+$/i.test(trimmed)) {
        return NextResponse.json(
          { error: "VM URL must start with http:// or https://" },
          { status: 400 },
        );
      }
      update.vmUrl = trimmed;
    } else {
      return NextResponse.json(
        { error: "Invalid vmUrl" },
        { status: 400 },
      );
    }
  }

  if (Object.keys(update).length === 0) {
    return NextResponse.json({ error: "Nothing to update" }, { status: 400 });
  }

  await db.update(team).set(update).where(eq(team.id, t.id));
  return NextResponse.json({ ok: true });
}

export async function DELETE(
  _req: Request,
  { params }: { params: Promise<{ slug: string }> },
) {
  const session = await getSession();
  if (!session) return NextResponse.json({ error: "unauthorized" }, { status: 401 });

  const { slug } = await params;
  const [t] = await db.select().from(team).where(eq(team.slug, slug)).limit(1);
  if (!t) return NextResponse.json({ error: "not found" }, { status: 404 });
  if (t.ownerId !== session.user.id)
    return NextResponse.json({ error: "forbidden" }, { status: 403 });

  await db.delete(team).where(eq(team.id, t.id));
  return NextResponse.json({ ok: true });
}
