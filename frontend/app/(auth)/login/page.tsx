"use client";

import { Button } from "@/components/ui/button";
import { authClient } from "@/lib/auth-client";
import { Github, Terminal } from "lucide-react";
import Link from "next/link";
import { useState } from "react";

function GoogleIcon({ className }: { className?: string }) {
  return (
    <svg className={className} viewBox="0 0 24 24" fill="currentColor">
      <path d="M21.35 11.1h-9.17v2.93h6.51c-.33 3.81-3.5 5.44-6.5 5.44C8.36 19.47 5 16.36 5 12a7.34 7.34 0 0 1 7.2-7.47c2.09 0 3.32.7 4.29 1.52l2.29-2.29A10.72 10.72 0 0 0 12.1 1C5.99 1 1 5.97 1 12s4.99 11 11.1 11c5.16 0 9.6-3.7 9.6-9.51 0-1.03-.11-1.71-.35-2.39Z" />
    </svg>
  );
}

export default function LoginPage() {
  const [loading, setLoading] = useState<string | null>(null);

  const signIn = async (provider: "github" | "google") => {
    setLoading(provider);
    try {
      await authClient.signIn.social({
        provider,
        callbackURL: "/leaderboard",
      });
    } finally {
      setLoading(null);
    }
  };

  return (
    <div className="relative min-h-screen flex items-center justify-center px-4 overflow-hidden">
      <div className="absolute inset-0 terminal-grid opacity-30 pointer-events-none" />
      <div className="scan-line absolute inset-0 pointer-events-none" />

      <div className="relative z-10 w-full max-w-md">
        <Link
          href="/"
          className="flex items-center justify-center gap-2 mb-12 group"
        >
          <Terminal className="h-4 w-4 text-primary transition-transform group-hover:rotate-12" />
          <span className="font-mono text-sm uppercase tracking-widest">
            polars<span className="text-primary">.</span>bench
          </span>
        </Link>

        <div className="bracket-border border border-border bg-card p-8">
          <div className="mb-8">
            <div className="font-mono text-[10px] uppercase tracking-widest text-muted-foreground mb-2">
              // authentication_required
            </div>
            <h1 className="font-display text-4xl">
              Enter the <span className="italic text-primary">arena</span>
            </h1>
            <p className="mt-3 text-sm text-muted-foreground">
              Sign in to create a team, submit a model, and watch the
              leaderboard climb.
            </p>
          </div>

          <div className="space-y-3">
            <Button
              variant="secondary"
              size="lg"
              className="w-full justify-start"
              onClick={() => signIn("github")}
              disabled={loading !== null}
            >
              <Github className="h-4 w-4" />
              {loading === "github" ? "Redirecting..." : "Continue with GitHub"}
            </Button>
            <Button
              variant="secondary"
              size="lg"
              className="w-full justify-start"
              onClick={() => signIn("google")}
              disabled={loading !== null}
            >
              <GoogleIcon className="h-4 w-4" />
              {loading === "google" ? "Redirecting..." : "Continue with Google"}
            </Button>
          </div>

          <div className="mt-8 pt-6 border-t border-border font-mono text-[10px] uppercase tracking-widest text-muted-foreground">
            // session persists 30 days
          </div>
        </div>
      </div>
    </div>
  );
}
