"use client";

import Link from "next/link";
import { usePathname, useRouter } from "next/navigation";
import { authClient, useSession } from "@/lib/auth-client";
import { Button } from "./ui/button";
import { Avatar, AvatarFallback, AvatarImage } from "./ui/avatar";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "./ui/dropdown-menu";
import { LogOut, Terminal, Trophy, Users, Upload, List } from "lucide-react";
import { cn } from "@/lib/utils";

const NAV = [
  { href: "/leaderboard", label: "Leaderboard", icon: Trophy },
  { href: "/teams", label: "Teams", icon: Users },
  { href: "/submissions", label: "Submissions", icon: List },
  { href: "/submit", label: "Submit", icon: Upload },
];

export function Navbar() {
  const { data, isPending } = useSession();
  const pathname = usePathname();
  const router = useRouter();

  return (
    <header className="relative z-10 border-b border-border bg-background/80 backdrop-blur">
      <div className="mx-auto flex h-14 max-w-7xl items-center gap-6 px-4 sm:px-6 lg:px-8">
        <Link href="/" className="flex items-center gap-2 group">
          <Terminal className="h-4 w-4 text-primary transition-transform group-hover:rotate-12" />
          <span className="font-mono text-sm uppercase tracking-widest">
            polars<span className="text-primary">.</span>bench
          </span>
        </Link>

        <nav className="hidden md:flex items-center gap-1">
          {NAV.map((item) => {
            const active =
              pathname === item.href || pathname.startsWith(item.href + "/");
            return (
              <Link
                key={item.href}
                href={item.href}
                className={cn(
                  "flex items-center gap-2 px-3 py-1.5 font-mono text-xs uppercase tracking-widest transition-colors",
                  active
                    ? "text-primary"
                    : "text-muted-foreground hover:text-foreground",
                )}
              >
                <item.icon className="h-3.5 w-3.5" />
                {item.label}
              </Link>
            );
          })}
        </nav>

        <div className="ml-auto flex items-center gap-2">
          {isPending ? (
            <div className="h-8 w-8 animate-pulse bg-muted" />
          ) : data?.user ? (
            <DropdownMenu>
              <DropdownMenuTrigger asChild>
                <button className="flex items-center gap-2 px-2 py-1 hover:bg-accent transition-colors">
                  <Avatar>
                    <AvatarImage src={data.user.image || undefined} />
                    <AvatarFallback>
                      {data.user.name?.slice(0, 2) || "??"}
                    </AvatarFallback>
                  </Avatar>
                  <span className="hidden sm:inline font-mono text-xs">
                    {data.user.name}
                  </span>
                </button>
              </DropdownMenuTrigger>
              <DropdownMenuContent align="end">
                <DropdownMenuLabel>{data.user.email}</DropdownMenuLabel>
                <DropdownMenuSeparator />
                <DropdownMenuItem asChild>
                  <Link href="/teams">My Team</Link>
                </DropdownMenuItem>
                <DropdownMenuSeparator />
                <DropdownMenuItem
                  onClick={async () => {
                    await authClient.signOut();
                    router.push("/");
                    router.refresh();
                  }}
                >
                  <LogOut className="h-3.5 w-3.5" /> Sign out
                </DropdownMenuItem>
              </DropdownMenuContent>
            </DropdownMenu>
          ) : (
            <Button size="sm" asChild>
              <Link href="/login">Sign in</Link>
            </Button>
          )}
        </div>
      </div>
    </header>
  );
}
