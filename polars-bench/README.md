# polars.bench

> Benchmark platform for the **Polars SLM Hackathon** — teams submit a GitHub repo, a GPU container runs 15 Polars questions against their model, and the leaderboard ranks everyone in real time over SSE.

```
 ┌─────────┐    POST /run-repo-stream     ┌──────────────┐
 │ Next.js │ ───────────────────────────► │  FastAPI BE  │
 │  (this) │                              │ (Nisseya/    │
 │         │ ◄─── event: question_result  │  hack_apr)   │
 │ Postgres│                              │  GPU runner  │
 └─────────┘                              └──────────────┘
      ▲
      │ SSE re-broadcast
      │
   ┌─────┐
   │ You │
   └─────┘
```

This repo is **frontend only**. It expects the FastAPI backend from [Nisseya/hack_apr](https://github.com/Nisseya/hack_apr) to be reachable, and uses its `/run-repo-stream` endpoint.

---

## Stack

- **Next.js 15** (App Router, React 19, Server Components, Route Handlers)
- **better-auth** with GitHub + Google OAuth
- **PostgreSQL 16** + **Drizzle ORM**
- **Tailwind CSS** + custom shadcn-style components
- **Docker Compose** for one-command deployment
- Phosphor-terminal aesthetic (dark, lime accents, IBM Plex Sans + JetBrains Mono + Instrument Serif)

---

## Features

| Area | What it does |
| --- | --- |
| **Auth** | Sign in with Google or GitHub. 30-day sessions. |
| **Teams** | Public team directory. Any logged-in user can create a team (becomes owner) or request to join another. Owner accepts/rejects requests. 1 team per user, enforced at DB level. |
| **Submissions** | Members pick `test` (localhost backend, full visibility) or `global` (remote backend, leaderboard-only). Only one running submission per team at a time. |
| **Live SSE** | Each submission streams from the FastAPI backend through a Next.js route that parses each `question_result`, persists it, updates aggregates, and re-emits to the browser. With heartbeats every 15s. |
| **Leaderboard** | Best-of submission per team, **global kind only**. Composite = `accuracy × 70 + perf × 30` where perf normalizes latency + peak memory. Auto-refreshes every 8s. |
| **Visibility** | Team members see full per-question detail. Outsiders viewing a `global` submission see only the aggregate score. |
| **Retry** | A failed submission can be retried — the stream route wipes previous partial results first. |

---

## Pages

| Path | Purpose |
| --- | --- |
| `/` | Landing |
| `/login` | OAuth |
| `/leaderboard` | Global ranking (public to logged-in users) |
| `/teams` | Team directory + create + request-to-join |
| `/teams/[slug]` | Team detail, members, pending requests (owner) |
| `/submit` | Create a submission |
| `/submissions` | Team's submission history |
| `/submissions/[id]` | Live SSE viewer — KPIs, per-question details, logs, retry |

---

## Quick start (local dev)

### Prerequisites
- Node.js 20+
- Docker + Docker Compose
- The FastAPI backend running locally on `:8000` (see [Nisseya/hack_apr](https://github.com/Nisseya/hack_apr))

### Setup

```bash
git clone <this-repo>
cd polars-bench

cp .env.example .env
# Edit .env:
#   - BETTER_AUTH_SECRET  → openssl rand -base64 32
#   - GITHUB_CLIENT_ID / GITHUB_CLIENT_SECRET
#   - GOOGLE_CLIENT_ID / GOOGLE_CLIENT_SECRET
#   - BACKEND_TEST_URL    → http://host.docker.internal:8000 (docker) or http://localhost:8000 (native)
#   - BACKEND_GLOBAL_URL  → fill in before the hackathon

npm install --legacy-peer-deps

# Start Postgres only
docker compose up -d db

# Generate + apply migrations
npm run db:generate
npm run db:migrate

# Dev server
npm run dev
# → http://localhost:3000
```

### OAuth callback URLs

Add these to your OAuth apps:

- **GitHub**: `http://localhost:3000/api/auth/callback/github`
- **Google**: `http://localhost:3000/api/auth/callback/google`

---

## Production (docker compose)

```bash
# Fill .env
docker compose up -d --build
# migrations run automatically on container start
# → http://localhost:3000
```

The web container uses `host-gateway` so `BACKEND_TEST_URL=http://host.docker.internal:8000` reaches a FastAPI running on the host.

---

## How the SSE proxy works

The critical route is [`app/api/submissions/[id]/stream/route.ts`](./app/api/submissions/%5Bid%5D/stream/route.ts):

1. Client `POST`s to `/api/submissions/[id]/stream`.
2. Route validates the user is a team member of the submission.
3. Resets any partial `benchmark_result` rows (retry-safe).
4. Opens a `fetch` to the FastAPI backend's `/run-repo-stream` with `{ repo_url }`.
5. Reads the response body as a stream, parses each SSE record (`event:` + `data:`).
6. On `status` → forwards as-is.
7. On `question_result` → persists one `benchmark_result` row, updates live aggregates on the `submission` row, re-emits to client.
8. On `done` → computes the final composite score, marks submission `done`.
9. On `error` → marks submission `failed`, stores the error message.
10. Emits `:heartbeat\n\n` every 15s to keep proxies from killing the connection.

The client in `app/(app)/submissions/[id]/page.tsx` opens the same stream, parses events the same way, and updates the UI progressively.

---

## Scoring

```ts
composite = accuracy × 70 + perf × 30

perf = 0.6 × perf_latency + 0.4 × perf_memory
perf_latency = clamp(1 - (avg_gen + avg_exec - 2s) / 58s,  0, 1)
perf_memory  = clamp(1 - (max_peak_mem_mb - 4096) / 36864, 0, 1)
```

Tweakable in [`lib/scoring.ts`](./lib/scoring.ts).

---

## Data model (summary)

```
user ─┬─< session, account  (better-auth)
      └─< team_member ──► team ─< submission ─< benchmark_result
                              └─< join_request
```

Enforcement:
- `team_member.user_id` is `UNIQUE` → one team per user
- `submission` has a `(status IN queued|running)` guard at insert time → one active run per team
- `benchmark_result(submission_id, question_id)` is `UNIQUE` → one row per question per submission

---

## Environment variables

| Var | Purpose |
| --- | --- |
| `DATABASE_URL` | Postgres connection string |
| `BETTER_AUTH_SECRET` | Random 32-byte secret |
| `BETTER_AUTH_URL` | Public URL of this app |
| `GITHUB_CLIENT_ID` / `GITHUB_CLIENT_SECRET` | OAuth credentials |
| `GOOGLE_CLIENT_ID` / `GOOGLE_CLIENT_SECRET` | OAuth credentials |
| `BACKEND_TEST_URL` | FastAPI backend for `test` submissions |
| `BACKEND_GLOBAL_URL` | FastAPI backend for `global` submissions |

---

## Project structure

```
polars-bench/
├── app/
│   ├── (app)/                    # authenticated pages (redirects to /login)
│   │   ├── layout.tsx
│   │   ├── teams/
│   │   ├── submit/
│   │   ├── submissions/
│   │   │   └── [id]/             # live SSE viewer
│   │   └── leaderboard/
│   ├── (auth)/login/
│   ├── api/
│   │   ├── auth/[...all]/        # better-auth handler
│   │   ├── me/
│   │   ├── teams/
│   │   ├── join-requests/
│   │   ├── submissions/
│   │   │   └── [id]/stream/      # SSE proxy (the interesting one)
│   │   └── leaderboard/
│   ├── globals.css
│   ├── layout.tsx                # fonts, dark theme
│   └── page.tsx                  # landing
├── components/
│   ├── ui/                       # shadcn-style primitives
│   ├── navbar.tsx
│   └── status-badge.tsx
├── db/
│   ├── schema.ts                 # Drizzle schema
│   └── index.ts
├── lib/
│   ├── auth.ts / auth-client.ts  # better-auth
│   ├── session.ts
│   ├── scoring.ts                # composite score
│   └── utils.ts
├── scripts/migrate.ts
├── docker-compose.yml
├── Dockerfile
└── .env.example
```

---

## Notes for the hackathon day

- Before the event, set `BACKEND_GLOBAL_URL` to the remote GPU backend.
- Create a **read-only** demo account and pin the leaderboard on the big screen.
- The leaderboard refreshes every 8s — no manual refresh needed.
- The `test` benchmark can be run as many times as teams want; the `global` benchmark should ideally be rate-limited (not implemented — queue is best-of, not count-limited).

---

## License

MIT.
