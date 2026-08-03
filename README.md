# hack-apr

Plateforme de benchmark pour le **Polars SLM Hackathon** : les équipes soumettent un repo GitHub, un runner GPU exécute des questions Polars contre leur modèle, et le leaderboard classe tout le monde en temps réel via SSE.

Ce repo contient **deux parties** :

| Partie | Dossier | Stack |
| --- | --- | --- |
| **Backend** | racine (`main.py`) | FastAPI + Polars |
| **Frontend** | `polars-bench/` | Next.js 15 + Postgres + Drizzle |

> Le frontend est un simple sous-dossier du même dépôt git (pas de repo imbriqué). Il attend le backend FastAPI de ce repo via son endpoint `/run-repo-stream`.

---

## Backend (FastAPI)

### Prérequis
- Python 3.12+ (voir `.python-version`)
- [uv](https://docs.astral.sh/uv/)

### Installation

```bash
uv sync
```

### Lancement

```bash
uv run uvicorn main:app --reload --port 8000
```

Le backend écoute alors sur `http://localhost:8000`.

> **Note GPU** : le backend s'appuie sur un environnement de base `/workspace/hack_apr_env` et des chemins `/workspace/*` (voir `main.py`). En local hors conteneur GPU, ces chemins doivent exister ou être adaptés.

---

## Frontend (Next.js)

Voir le [README détaillé du frontend](./polars-bench/README.md) pour toutes les fonctionnalités.

### Prérequis
- Node.js 20+
- Docker + Docker Compose
- Le backend FastAPI qui tourne sur `:8000`

### Lancement (dev)

```bash
cd polars-bench

cp .env.example .env
# Renseigner BETTER_AUTH_SECRET, les OAuth, etc.

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

### Lancement (production, docker compose)

```bash
cd polars-bench
docker compose up -d --build
# → http://localhost:3000
```

---

## Scripts CLI (benchmark)

Deux scripts autonomes permettent de lancer un benchmark sur un repo soumis :

```bash
# Benchmark "public" (select, filters, joins, ...) — défaut: select
uv run python c_run_bench.py <repo_url> --benchmark select

# Benchmark final (scoring officiel)
uv run python c_run_final.py <repo_url> --secret <secret>
```

---

## Structure

```
hack-apr/
├── main.py                 # Backend FastAPI (endpoints /run-repo-stream, /submit_final)
├── helpers.py              # Exécution de code, métriques (RAM/GPU)
├── questions.py            # Chargement des questions de benchmark
├── generate_dataset.py     # Génération des jeux de données
├── c_run_bench.py          # CLI: benchmark public sur un repo
├── c_run_final.py          # CLI: benchmark final (scoring)
├── data/                   # Benchmarks JSON (questions + gold code)
└── polars-bench/           # Frontend Next.js
```
