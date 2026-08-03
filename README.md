# hack-apr

Plateforme de benchmark pour le **Polars SLM Hackathon** : les équipes soumettent un repo GitHub, un runner GPU exécute des questions Polars contre leur modèle, et le leaderboard classe tout le monde en temps réel via SSE.

Monorepo avec deux parties clairement séparées :

| Partie | Dossier | Stack |
| --- | --- | --- |
| **Backend** | [`backend/`](./backend) | FastAPI + Polars |
| **Frontend** | [`frontend/`](./frontend) | Next.js 15 + Postgres + Drizzle |

```
hack-apr/
├── backend/            # API FastAPI (évaluation des soumissions)
│   ├── main.py         #   endpoints /run-repo-stream, /submit_final
│   ├── helpers.py      #   exécution de code + métriques RAM/GPU
│   ├── questions.py    #   chargement des questions
│   ├── generate_dataset.py
│   ├── c_run_bench.py  #   CLI: benchmark public
│   ├── c_run_final.py  #   CLI: benchmark final (scoring)
│   ├── data/           #   benchmarks JSON (questions + gold code)
│   └── Dockerfile
├── frontend/           # App Next.js (leaderboard, soumissions, SSE)
│   ├── app/
│   ├── components/
│   ├── db/
│   ├── lib/
│   └── Dockerfile
├── docker-compose.yml  # orchestration backend + frontend + db
├── .env.example
└── README.md
```

---

## Démarrage rapide (Docker)

Le plus simple : tout lancer d'un coup avec Docker Compose.

```bash
cp .env.example .env
# Renseigner BETTER_AUTH_SECRET + les identifiants OAuth

docker compose up -d --build
```

Ça lance :
- **PostgreSQL** sur `:5432`
- **Backend FastAPI** sur `http://localhost:8000`
- **Frontend Next.js** sur `http://localhost:3000`

> Le frontend attend le backend via `BACKEND_TEST_URL=http://backend:8000` (nom de service du compose).

---

## Développement local

### Backend

```bash
cd backend
uv sync
uv run uvicorn main:app --reload --port 8000
```

> **Note GPU** : le backend s'appuie sur un environnement de base `/workspace/hack_apr_env` et des chemins `/workspace/*` (voir `main.py`). En local hors conteneur GPU, ces chemins doivent exister ou être adaptés.

### Frontend

```bash
cd frontend
cp .env.example .env
npm install --legacy-peer-deps

# Postgres uniquement
docker compose up -d db

# Migrations
npm run db:generate
npm run db:migrate

# Serveur de dev
npm run dev
# → http://localhost:3000
```

Voir le [README du frontend](./frontend/README.md) pour toutes les fonctionnalités.

---

## Scripts CLI (benchmark)

Deux scripts autonomes permettent de lancer un benchmark sur un repo soumis :

```bash
# Benchmark "public" (select, filters, joins, ...) — défaut: select
uv run python c_run_bench.py <repo_url> --benchmark select

# Benchmark final (scoring officiel)
uv run python c_run_final.py <repo_url> --secret <secret>
```
