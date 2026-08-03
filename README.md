# hack-apr

Plateforme de benchmark pour le **Polars SLM Hackathon** : les équipes soumettent un repo GitHub, un runner GPU exécute des questions Polars contre leur modèle, et le leaderboard classe tout le monde en temps réel via SSE.

Monorepo avec deux parties clairement séparées :

| Partie | Dossier | Stack |
| --- | --- | --- |
| **Backend / orchestrateur** | [`backend/`](./backend) | FastAPI + Polars |
| **Frontend** | [`frontend/`](./frontend) | Next.js 15 + Postgres + Drizzle |
| **Worker GPU** | [`worker/`](./worker) | Image Docker GPU (RunPod) |

```
hack-apr/
├── backend/            # Orchestrateur FastAPI (routage vers le pool GPU)
│   ├── main.py         #   endpoints /run-repo-stream, /submit_final (worker)
│   ├── pool.py         #   app orchestrateur (forwarding SSE + admin /pool)
│   ├── gpu_pool.py     #   gestion du pool de pods GPU (scaling manuel)
│   ├── runpod_client.py#   client API RunPod (créer/lister/terminer des pods)
│   ├── provision_pool.py # CLI: builder l'image + provisionner le pool
│   ├── helpers.py      #   exécution de code + métriques RAM/GPU
│   ├── questions.py    #   chargement des questions
│   ├── generate_dataset.py
│   ├── c_run_bench.py  #   CLI: benchmark public
│   ├── c_run_final.py  #   CLI: benchmark final (scoring)
│   ├── data/           #   benchmarks JSON (questions + gold code)
│   └── Dockerfile
├── worker/             # Image GPU (torch/transformers pré-installés)
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

## Pool de GPUs (RunPod)

Le backend peut fonctionner en **orchestrateur** : au lieu d'exécuter les benchmarks
lui-même, il maintient un **pool de pods GPU chauds** (modèles déjà en mémoire) et
achemine chaque soumission vers un pod libre, en relayant le flux SSE.

> Pourquoi des pods chauds plutôt que du serverless ? Le serverless re-télécharge
> les modèles à chaque cold start (coûteux et lent). Un pool de pods dédiés garde
> les modèles en mémoire et le cache de dépendances sur un volume persistant.

### 1. Builder & pousser l'image worker

```bash
# Depuis la racine du repo
RUNPOD_API_KEY=... python backend/provision_pool.py --push --size 2
```

### 2. Lancer l'orchestrateur

```bash
cd backend
RUNPOD_API_KEY=... RUNPOD_POOL_SIZE=2 uv run uvicorn pool:app --port 8000
```

### 3. Scaling manuel

```bash
# Inspecter le pool
curl http://localhost:8000/pool/health

# Monter à 3 workers
curl -X POST http://localhost:8000/pool/scale -H 'Content-Type: application/json' -d '{"size": 3}'

# Descendre à 1 worker (les pods excédentaires sont terminés)
curl -X POST http://localhost:8000/pool/scale -H 'Content-Type: application/json' -d '{"size": 1}'
```

### Variables d'environnement

| Var | Rôle |
| --- | --- |
| `RUNPOD_API_KEY` | Clé API RunPod (gestion des pods) |
| `RUNPOD_POOL_SIZE` | Nombre de pods chauds à maintenir |
| `RUNPOD_POOL_IMAGE` | Image worker (défaut `nisseya/hack-apr-worker:latest`) |
| `RUNPOD_POOL_GPU` | Type de GPU (défaut `A100-80GB`) |
| `RUNPOD_POOL_VOLUME_SIZE_GB` | Taille du volume persistant (cache modèles/deps) |

---

## Scripts CLI (benchmark)

Deux scripts autonomes permettent de lancer un benchmark sur un repo soumis :

```bash
# Benchmark "public" (select, filters, joins, ...) — défaut: select
uv run python c_run_bench.py <repo_url> --benchmark select

# Benchmark final (scoring officiel)
uv run python c_run_final.py <repo_url> --secret <secret>
```
