"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { Button } from "@/components/ui/button";
import { Input, Textarea } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Badge } from "@/components/ui/badge";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import { Plus, Users, UserPlus, Crown } from "lucide-react";
import { toast } from "@/components/ui/toaster";
import { relativeTime } from "@/lib/utils";
import { useSession } from "@/lib/auth-client";

type TeamRow = {
  id: string;
  name: string;
  slug: string;
  description: string | null;
  ownerId: string;
  createdAt: string;
  memberCount: number;
};

export default function TeamsPage() {
  const { data: session } = useSession();
  const router = useRouter();
  const [teams, setTeams] = useState<TeamRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [myTeamId, setMyTeamId] = useState<string | null>(null);

  const load = async () => {
    const res = await fetch("/api/teams", { cache: "no-store" });
    const data = await res.json();
    setTeams(data.teams ?? []);
    // find own team from memberships
    const meRes = await fetch("/api/me", { cache: "no-store" });
    if (meRes.ok) {
      const me = await meRes.json();
      setMyTeamId(me?.team?.id ?? null);
    }
    setLoading(false);
  };

  useEffect(() => {
    load();
  }, []);

  return (
    <div className="mx-auto max-w-7xl px-4 sm:px-6 lg:px-8 py-10">
      <div className="flex items-end justify-between mb-10">
        <div>
          <div className="font-mono text-[10px] uppercase tracking-widest text-muted-foreground">
            // roster
          </div>
          <h1 className="font-display text-5xl mt-1">
            Teams <span className="italic text-muted-foreground">&</span>{" "}
            rosters
          </h1>
        </div>
        {session && !myTeamId && <CreateTeamDialog onCreated={load} />}
      </div>

      {loading ? (
        <div className="text-muted-foreground font-mono text-xs uppercase tracking-widest">
          Loading...
        </div>
      ) : teams.length === 0 ? (
        <div className="border border-dashed border-border p-12 text-center font-mono text-sm text-muted-foreground">
          No teams yet. Be the first.
        </div>
      ) : (
        <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
          {teams.map((t) => (
            <TeamCard
              key={t.id}
              team={t}
              isMine={myTeamId === t.id}
              userHasTeam={!!myTeamId}
              signedIn={!!session}
            />
          ))}
        </div>
      )}
    </div>
  );
}

function TeamCard({
  team,
  isMine,
  userHasTeam,
  signedIn,
}: {
  team: TeamRow;
  isMine: boolean;
  userHasTeam: boolean;
  signedIn: boolean;
}) {
  const [open, setOpen] = useState(false);
  const [message, setMessage] = useState("");
  const [sending, setSending] = useState(false);
  const router = useRouter();

  const requestJoin = async () => {
    setSending(true);
    try {
      const res = await fetch(`/api/teams/${team.slug}/join`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ message }),
      });
      if (!res.ok) {
        const d = await res.json().catch(() => ({}));
        toast({
          title: "Request failed",
          description: d.error ?? "Something went wrong",
          variant: "destructive",
        });
      } else {
        toast({ title: "Request sent", description: "Waiting for the owner" });
        setOpen(false);
      }
    } finally {
      setSending(false);
    }
  };

  return (
    <Card className="group hover:border-primary/50 transition-colors">
      <CardHeader>
        <div className="flex items-start justify-between">
          <CardTitle className="truncate">{team.name}</CardTitle>
          {isMine && (
            <Badge variant="default">
              <Crown className="h-3 w-3" /> mine
            </Badge>
          )}
        </div>
        <CardDescription>
          {team.description || "No description"}
        </CardDescription>
      </CardHeader>
      <CardContent className="flex items-center justify-between">
        <div className="flex items-center gap-3 text-xs font-mono uppercase tracking-widest text-muted-foreground">
          <span className="flex items-center gap-1">
            <Users className="h-3 w-3" /> {team.memberCount}
          </span>
          <span>·</span>
          <span>{relativeTime(team.createdAt)}</span>
        </div>
        <div className="flex gap-2">
          <Button variant="ghost" size="sm" asChild>
            <Link href={`/teams/${team.slug}`}>View</Link>
          </Button>
          {signedIn && !isMine && !userHasTeam && (
            <Dialog open={open} onOpenChange={setOpen}>
              <DialogTrigger asChild>
                <Button size="sm" variant="outline">
                  <UserPlus className="h-3 w-3" /> Join
                </Button>
              </DialogTrigger>
              <DialogContent>
                <DialogHeader>
                  <DialogTitle>Request to join {team.name}</DialogTitle>
                  <DialogDescription>
                    The team owner will review your request.
                  </DialogDescription>
                </DialogHeader>
                <div className="space-y-2">
                  <Label htmlFor="msg">Message (optional)</Label>
                  <Textarea
                    id="msg"
                    placeholder="Hey, I want to join because..."
                    value={message}
                    onChange={(e) => setMessage(e.target.value)}
                    maxLength={280}
                  />
                </div>
                <DialogFooter>
                  <Button
                    variant="outline"
                    onClick={() => setOpen(false)}
                    disabled={sending}
                  >
                    Cancel
                  </Button>
                  <Button onClick={requestJoin} disabled={sending}>
                    {sending ? "Sending..." : "Send request"}
                  </Button>
                </DialogFooter>
              </DialogContent>
            </Dialog>
          )}
        </div>
      </CardContent>
    </Card>
  );
}

function CreateTeamDialog({ onCreated }: { onCreated: () => void }) {
  const [open, setOpen] = useState(false);
  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [creating, setCreating] = useState(false);
  const router = useRouter();

  const create = async () => {
    setCreating(true);
    try {
      const res = await fetch("/api/teams", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name, description }),
      });
      const data = await res.json();
      if (!res.ok) {
        toast({
          title: "Create failed",
          description: data.error ?? "Something went wrong",
          variant: "destructive",
        });
        return;
      }
      toast({ title: "Team created" });
      setOpen(false);
      setName("");
      setDescription("");
      onCreated();
      router.push(`/teams/${data.team.slug}`);
    } finally {
      setCreating(false);
    }
  };

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <Button>
          <Plus className="h-4 w-4" /> Create team
        </Button>
      </DialogTrigger>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Create a new team</DialogTitle>
          <DialogDescription>
            You'll be the owner. A user can only belong to one team at a time.
          </DialogDescription>
        </DialogHeader>
        <div className="space-y-3">
          <div className="space-y-2">
            <Label htmlFor="n">Team name</Label>
            <Input
              id="n"
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="The Lazy Frames"
              maxLength={48}
            />
          </div>
          <div className="space-y-2">
            <Label htmlFor="d">Description</Label>
            <Textarea
              id="d"
              value={description}
              onChange={(e) => setDescription(e.target.value)}
              placeholder="Optional"
              maxLength={280}
            />
          </div>
        </div>
        <DialogFooter>
          <Button
            variant="outline"
            onClick={() => setOpen(false)}
            disabled={creating}
          >
            Cancel
          </Button>
          <Button onClick={create} disabled={creating || name.length < 2}>
            {creating ? "Creating..." : "Create"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
