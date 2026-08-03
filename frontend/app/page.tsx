import Link from "next/link";
import { Button } from "@/components/ui/button";
import { getSession } from "@/lib/session";
import { ArrowRight, Cpu, Gauge, GitBranch, Terminal } from "lucide-react";

export default async function HomePage() {
  const session = await getSession();

  return (
    <div className="relative min-h-screen overflow-hidden">
      {/* Grid background */}
      <div className="absolute inset-0 terminal-grid opacity-30" />
      <div className="absolute inset-x-0 top-0 h-[600px] bg-gradient-to-b from-primary/5 via-transparent to-transparent pointer-events-none" />

      <header className="relative z-10 border-b border-border">
        <div className="mx-auto flex h-14 max-w-7xl items-center justify-between px-4 sm:px-6 lg:px-8">
          <div className="flex items-center gap-2">
            <Terminal className="h-4 w-4 text-primary" />
            <span className="font-mono text-sm uppercase tracking-widest">
              polars<span className="text-primary">.</span>bench
            </span>
          </div>
          <div className="flex items-center gap-3">
            <span className="hidden sm:inline font-mono text-xs uppercase tracking-widest text-muted-foreground">
              // SLM Hackathon
            </span>
            <Button size="sm" asChild>
              <Link href={session ? "/leaderboard" : "/login"}>
                {session ? "Enter" : "Sign in"}
              </Link>
            </Button>
          </div>
        </div>
      </header>

      <section className="relative z-10 mx-auto max-w-7xl px-4 sm:px-6 lg:px-8 py-20 md:py-32">
        <div className="grid md:grid-cols-2 gap-12 items-end">
          <div>
            <div className="inline-flex items-center gap-2 border border-primary/30 bg-primary/5 px-3 py-1 mb-8">
              <span className="inline-block h-1.5 w-1.5 rounded-full bg-primary animate-pulse-dot" />
              <span className="font-mono text-[10px] uppercase tracking-widest text-primary">
                benchmark runner online
              </span>
            </div>

            <h1 className="font-display text-5xl md:text-7xl lg:text-8xl leading-[0.95] tracking-tight">
              Small Models.
              <br />
              <span className="italic text-primary text-glow">Big Queries.</span>
            </h1>

            <p className="mt-6 max-w-lg font-mono text-sm leading-relaxed text-muted-foreground">
              Submit your SLM to the arena. We spawn a GPU container, stream 15
              Polars questions through it, execute the generated code against
              gold outputs, and rank your team in real time.
            </p>

            <div className="mt-10 flex flex-wrap gap-3">
              <Button size="lg" asChild>
                <Link href={session ? "/submit" : "/login"}>
                  Submit a repo <ArrowRight className="h-4 w-4" />
                </Link>
              </Button>
              <Button size="lg" variant="outline" asChild>
                <Link href="/leaderboard">View leaderboard</Link>
              </Button>
            </div>
          </div>

          <div className="relative">
            <div className="bracket-border border border-border bg-card p-6 font-mono text-xs leading-relaxed">
              <div className="text-muted-foreground mb-2">
                $ polars-bench run --repo=https://github.com/team/slm
              </div>
              <div className="space-y-1.5">
                <div>
                  <span className="text-primary">[01/15]</span> question_started
                  <span className="text-muted-foreground ml-2">
                    Count premium customers by country...
                  </span>
                </div>
                <div>
                  <span className="text-primary">[01/15]</span> question_result
                  <span className="text-primary ml-2">✓ exact_match</span>
                  <span className="text-muted-foreground ml-2">
                    gen=2.1s · peak_ram=1.2GB
                  </span>
                </div>
                <div>
                  <span className="text-primary">[02/15]</span> question_started
                  <span className="text-muted-foreground ml-2">
                    Compute total revenue...
                  </span>
                </div>
                <div>
                  <span className="text-primary">[02/15]</span> question_result
                  <span className="text-destructive ml-2">✗ mismatch</span>
                </div>
                <div className="text-muted-foreground">
                  [03/15] question_started{" "}
                  <span className="inline-block animate-pulse-dot">_</span>
                </div>
              </div>
            </div>
          </div>
        </div>

        {/* Feature row */}
        <div className="mt-24 grid sm:grid-cols-3 gap-0 border-t border-b border-border">
          {[
            {
              icon: GitBranch,
              title: "Repo-based",
              desc: "Submit a GitHub repo. We clone, install, and spin up your inference server on GPU.",
            },
            {
              icon: Gauge,
              title: "Two benchmarks",
              desc: "Test with full visibility. Global with score-only for fair ranking.",
            },
            {
              icon: Cpu,
              title: "Live stream",
              desc: "15 questions, each streamed via SSE. Watch your model think in real time.",
            },
          ].map((f, i) => (
            <div
              key={f.title}
              className={`p-8 ${i < 2 ? "sm:border-r sm:border-border" : ""}`}
            >
              <f.icon className="h-5 w-5 text-primary mb-4" />
              <h3 className="font-mono text-xs uppercase tracking-widest mb-2">
                {f.title}
              </h3>
              <p className="text-sm text-muted-foreground leading-relaxed">
                {f.desc}
              </p>
            </div>
          ))}
        </div>
      </section>

      <footer className="relative z-10 border-t border-border">
        <div className="mx-auto max-w-7xl px-4 py-6 font-mono text-[10px] uppercase tracking-widest text-muted-foreground flex justify-between">
          <span>// polars.bench</span>
          <span>hackathon_apr</span>
        </div>
      </footer>
    </div>
  );
}
