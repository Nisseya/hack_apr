"use client";

import { use, useEffect, useState } from "react";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { Avatar, AvatarFallback, AvatarImage } from "@/components/ui/avatar";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { toast } from "@/components/ui/toaster";
import { cn, relativeTime } from "@/lib/utils";
import {
  Check,
  Crown,
  Eye,
  EyeOff,
  LogOut,
  Pencil,
  Server,
  Trash2,
  X,
} from "lucide-react";

type TeamDetail = {
  team: {
    id: string;
    name: string;
    slug: string;
    description: string | null;
    ownerId: string;
    createdAt: string;
    vmUrl: string | null;
    vmUrlConfigured: boolean;
  };
  members: Array<{
    userId: string;
    role: string;
    joinedAt: string;
    name: string;
    email: string;
    image: string | null;
  }>;
  joinRequests: Array<{
    id: string;
    status: string;
    message: string | null;
    createdAt: string;
    userId: string;
    userName: string;
    userImage: string | null;
    userEmail: string;
  }>;
  currentUserRole: "owner" | "member" | null;
  hasPendingRequest: boolean;
};

export default function TeamDetailPage({
  params,
}: {
  params: Promise<{ slug: string }>;
}) {
  const { slug } = use(params);
  const [data, setData] = useState<TeamDetail | null>(null);
  const [loading, setLoading] = useState(true);
  const router = useRouter();

  const load = async () => {
    const res = await fetch(`/api/teams/${slug}`, { cache: "no-store" });
    if (!res.ok) {
      setData(null);
    } else {
      setData(await res.json());
    }
    setLoading(false);
  };

  useEffect(() => {
    load();
  }, [slug]);

  if (loading)
    return (
      <div className="mx-auto max-w-5xl px-4 py-10 font-mono text-xs uppercase tracking-widest text-muted-foreground">
        Loading team...
      </div>
    );
  if (!data)
    return (
      <div className="mx-auto max-w-5xl px-4 py-10 font-mono text-xs uppercase tracking-widest">
        Team not found.{" "}
        <Link href="/teams" className="text-primary underline">
          back to teams
        </Link>
      </div>
    );

  const { team, members, joinRequests, currentUserRole } = data;

  const resolveRequest = async (id: string, action: "accept" | "reject") => {
    const res = await fetch(`/api/join-requests/${id}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ action }),
    });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) {
      toast({
        title: "Failed",
        description: json.error ?? "error",
        variant: "destructive",
      });
    } else {
      toast({
        title: action === "accept" ? "Accepted" : "Rejected",
      });
      load();
    }
  };

  const leave = async () => {
    if (!confirm("Leave this team?")) return;
    const res = await fetch(`/api/teams/${slug}/leave`, { method: "POST" });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) {
      toast({
        title: "Failed",
        description: json.error ?? "error",
        variant: "destructive",
      });
    } else {
      toast({ title: "Left team" });
      router.push("/teams");
      router.refresh();
    }
  };

  return (
    <div className="mx-auto max-w-5xl px-4 sm:px-6 lg:px-8 py-10">
      <Link
        href="/teams"
        className="font-mono text-[10px] uppercase tracking-widest text-muted-foreground hover:text-primary"
      >
        ← teams
      </Link>

      <div className="mt-4 flex items-start justify-between">
        <div>
          <div className="font-mono text-[10px] uppercase tracking-widest text-muted-foreground">
            // team_roster
          </div>
          <h1 className="font-display text-5xl mt-1">{team.name}</h1>
          {team.description && (
            <p className="mt-3 text-sm text-muted-foreground max-w-xl">
              {team.description}
            </p>
          )}
          <div className="mt-3 flex items-center gap-2 font-mono text-[10px] uppercase tracking-widest text-muted-foreground">
            <span>created {relativeTime(team.createdAt)}</span>
            <span>·</span>
            <span>{members.length} members</span>
          </div>
        </div>

        {currentUserRole && (
          <Button variant="outline" size="sm" onClick={leave}>
            <LogOut className="h-3 w-3" />
            {currentUserRole === "owner" ? "Delete" : "Leave"}
          </Button>
        )}
      </div>

      {currentUserRole === "owner" && joinRequests.length > 0 && (
        <Card className="mt-10">
          <CardHeader>
            <CardTitle>Pending requests ({joinRequests.length})</CardTitle>
          </CardHeader>
          <CardContent className="space-y-2 p-0">
            {joinRequests.map((r) => (
              <div
                key={r.id}
                className="flex items-center gap-4 p-4 border-t border-border first:border-0"
              >
                <Avatar>
                  <AvatarImage src={r.userImage || undefined} />
                  <AvatarFallback>{r.userName.slice(0, 2)}</AvatarFallback>
                </Avatar>
                <div className="flex-1 min-w-0">
                  <div className="font-mono text-sm">{r.userName}</div>
                  {r.message && (
                    <div className="text-xs text-muted-foreground line-clamp-1">
                      "{r.message}"
                    </div>
                  )}
                </div>
                <span className="font-mono text-[10px] uppercase tracking-widest text-muted-foreground">
                  {relativeTime(r.createdAt)}
                </span>
                <div className="flex gap-1">
                  <Button
                    size="sm"
                    variant="outline"
                    onClick={() => resolveRequest(r.id, "reject")}
                  >
                    <X className="h-3 w-3" />
                  </Button>
                  <Button
                    size="sm"
                    onClick={() => resolveRequest(r.id, "accept")}
                  >
                    <Check className="h-3 w-3" />
                  </Button>
                </div>
              </div>
            ))}
          </CardContent>
        </Card>
      )}

      {currentUserRole && (
        <VmUrlSection
          team={team}
          canEdit={currentUserRole === "owner"}
          onUpdated={load}
        />
      )}

      <Card className="mt-6">
        <CardHeader>
          <CardTitle>Members</CardTitle>
        </CardHeader>
        <CardContent className="p-0">
          {members.map((m) => (
            <div
              key={m.userId}
              className="flex items-center gap-4 p-4 border-t border-border first:border-0"
            >
              <Avatar>
                <AvatarImage src={m.image || undefined} />
                <AvatarFallback>{m.name.slice(0, 2)}</AvatarFallback>
              </Avatar>
              <div className="flex-1 min-w-0">
                <div className="font-mono text-sm">{m.name}</div>
                <div className="text-xs text-muted-foreground truncate">
                  {m.email}
                </div>
              </div>
              {m.role === "owner" && (
                <Badge variant="default">
                  <Crown className="h-3 w-3" /> owner
                </Badge>
              )}
              <span className="font-mono text-[10px] uppercase tracking-widest text-muted-foreground">
                {relativeTime(m.joinedAt)}
              </span>
            </div>
          ))}
        </CardContent>
      </Card>
    </div>
  );
}

function VmUrlSection({
  team,
  canEdit,
  onUpdated,
}: {
  team: TeamDetail["team"];
  canEdit: boolean;
  onUpdated: () => void;
}) {
  const [editing, setEditing] = useState(false);
  const [value, setValue] = useState(team.vmUrl ?? "");
  const [reveal, setReveal] = useState(false);
  const [saving, setSaving] = useState(false);

  const save = async () => {
    setSaving(true);
    try {
      const res = await fetch(`/api/teams/${team.slug}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ vmUrl: value.trim() || null }),
      });
      const json = await res.json().catch(() => ({}));
      if (!res.ok) {
        toast({
          title: "Save failed",
          description: json.error ?? "Error",
          variant: "destructive",
        });
        return;
      }
      toast({ title: "VM URL updated" });
      setEditing(false);
      onUpdated();
    } finally {
      setSaving(false);
    }
  };

  const configured = !!team.vmUrl;

  return (
    <Card className="mt-10">
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Server className="h-3.5 w-3.5" /> Team VM URL
        </CardTitle>
        <CardDescription>
          The FastAPI backend endpoint your team's VM exposes. Used when you
          run a <span className="text-primary">test</span> submission. Only
          team members can see this value.
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-3">
        {editing ? (
          <>
            <Label htmlFor="vm">URL</Label>
            <Input
              id="vm"
              value={value}
              onChange={(e) => setValue(e.target.value)}
              placeholder="https://my-vm.example.com:8000"
              autoFocus
            />
            <div className="flex gap-2 justify-end">
              <Button
                variant="outline"
                size="sm"
                onClick={() => {
                  setEditing(false);
                  setValue(team.vmUrl ?? "");
                }}
                disabled={saving}
              >
                Cancel
              </Button>
              <Button size="sm" onClick={save} disabled={saving}>
                {saving ? "Saving..." : "Save"}
              </Button>
            </div>
          </>
        ) : (
          <div className="flex items-center gap-3">
            <div className="flex-1 min-w-0">
              {configured ? (
                <div className="flex items-center gap-3">
                  <span
                    className={cn(
                      "inline-block h-1.5 w-1.5 rounded-full bg-primary animate-pulse-dot",
                    )}
                  />
                  <code
                    className={cn(
                      "font-mono text-xs truncate block max-w-full",
                      !reveal && "blur-sm select-none",
                    )}
                    title={reveal ? team.vmUrl ?? "" : "Click 'show' to reveal"}
                  >
                    {team.vmUrl}
                  </code>
                  <Button
                    size="sm"
                    variant="ghost"
                    onClick={() => setReveal(!reveal)}
                  >
                    {reveal ? (
                      <>
                        <EyeOff className="h-3 w-3" /> hide
                      </>
                    ) : (
                      <>
                        <Eye className="h-3 w-3" /> show
                      </>
                    )}
                  </Button>
                </div>
              ) : (
                <span className="font-mono text-xs text-muted-foreground uppercase tracking-widest">
                  // no vm configured yet
                </span>
              )}
            </div>
            {canEdit && (
              <Button
                size="sm"
                variant="outline"
                onClick={() => {
                  setValue(team.vmUrl ?? "");
                  setEditing(true);
                }}
              >
                <Pencil className="h-3 w-3" /> {configured ? "Edit" : "Set"}
              </Button>
            )}
          </div>
        )}
      </CardContent>
    </Card>
  );
}
