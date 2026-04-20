# Mode d'emploi — Benchmarking SLMs with Polars

## Mode opératoire en 1 page (TLDR)

### Objectif
- Lancer un benchmark de génération de code et voir les résultats dans un tableau simple.

### Étapes (5 minutes)
1. Ouvrir un terminal à la racine du projet.
2. Installer les dépendances :

```bash
uv sync
```

3. Construire l'image Docker d'exécution :

```bash
docker build -f dockerfiles/Dockerfile.executor -t polars-executor:latest .
```

4. Démarrer le backend :

```bash
uv run uvicorn main:app --host 0.0.0.0 --port 8000
```

5. Dans un 2e terminal, démarrer l'interface :

```bash
uv run streamlit run frontend/main.py
```

6. Ouvrir l'URL Streamlit (souvent `http://localhost:8501`) puis :
   - aller sur **Créer une nouvelle expérience**,
   - choisir **Provider** (le plus simple),
   - choisir provider + modèle,
   - coller la clé API,
   - cliquer **Lancer l'expérience**.

### Où lire le résultat
- Page **Historique des expériences** : score de succès, exact match, temps, détails par question.

### Problèmes fréquents
- Si Docker n'est pas installé, rien ne démarre.
- Si le backend n'est pas lancé, l'interface ne peut pas exécuter de benchmark.

---

Ce projet permet de **benchmarker des modèles de génération de code Python/Polars** en deux étapes :
1. génération de code pour un jeu de questions,
2. exécution de ce code dans un conteneur isolé, puis comparaison des résultats.

Il fournit :
- un **backend FastAPI** (`main.py`) qui orchestre les runs,
- un **frontend Streamlit** (`frontend/main.py`) pour lancer les expériences et consulter l'historique.

---

## 1) Prérequis

### Système
- Python **3.13+**
- Docker installé et accessible via la commande `docker`
- (Optionnel) GPU NVIDIA + runtime Docker GPU pour les runs repo (car `--gpus all` est utilisé)

### Dépendances Python
Le projet utilise `uv` (recommandé), mais vous pouvez aussi utiliser `pip`.

Installation rapide avec `uv` :

```bash
uv sync
```

Alternative avec `pip` :

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

---

## 2) Préparer les images Docker

Avant tout lancement de benchmark, construisez au moins l'image de l'exécuteur :

```bash
docker build -f dockerfiles/Dockerfile.executor -t polars-executor:latest .
```

Pour le mode `run-repo`, le backend lance aussi l'image `gpu-fastapi-base:cu121`. Assurez-vous qu'elle est disponible localement.

---

## 3) Démarrer le backend

Depuis la racine du projet :

```bash
uv run uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

Vérification :
- Documentation Swagger : `http://localhost:8000/docs`

Endpoints principaux :
- `POST /run-repo`
- `POST /run-provider-experiment`

---

## 4) Démarrer l'interface utilisateur (frontend)

Dans un autre terminal :

```bash
uv run streamlit run frontend/main.py
```

Puis ouvrez l'URL affichée par Streamlit (souvent `http://localhost:8501`).

Le frontend parle au backend sur `http://localhost:8000`.

---

## 5) Utilisation côté usager

## A. Lancer une expérience "Repo"
1. Allez dans **Créer une nouvelle expérience**.
2. Sélectionnez le mode **Repo**.
3. Renseignez :
   - **Nom de l'expérience**,
   - **URL du repo** (GitHub).
4. Cliquez sur **Lancer l'expérience**.

Ce qui se passe :
- le backend clone le repo,
- démarre un conteneur générateur,
- envoie chaque question,
- exécute chaque réponse de code dans le conteneur `polars-executor`,
- calcule succès et exact match.

## B. Lancer une expérience "Provider"
1. Allez dans **Créer une nouvelle expérience**.
2. Sélectionnez le mode **Provider**.
3. Choisissez :
   - provider (`openai`, `xai`, `cerebras`),
   - modèle,
   - API key,
   - température,
   - max tokens,
   - prompt système additionnel (facultatif).
4. Lancez l'expérience.

## C. Mode "Hugging Face"
Le formulaire existe dans l'UI, mais le backend ne fournit pas encore d'endpoint dédié ; ce mode est informatif pour l'instant.

---

## 6) Lire les résultats

Dans **Historique des expériences**, vous obtenez :
- tableau comparatif des runs,
- stats globales : succès, exact match, temps génération/exécution, RAM/GPU max,
- détail question par question :
  - code généré,
  - `stdout`,
  - `stderr`,
  - statut d'exécution.

L'historique est stocké localement dans :
- `benchmark_history_2.json`

---

## 7) Dépannage rapide

- **`docker: command not found`** : installez Docker et relancez.
- **Erreur image introuvable** (`polars-executor:latest` ou `gpu-fastapi-base:cu121`) : construisez/téléchargez l'image manquante.
- **`/run-repo` échoue au démarrage** :
  - vérifier que l'URL du repo est accessible,
  - vérifier que le repo contient un service FastAPI lançable via `uvicorn main:app`.
- **Frontend ne répond pas** : vérifier que le backend tourne sur `localhost:8000`.

---

## 8) Exemple minimal d'appel API

### `POST /run-provider-experiment`

```bash
curl -X POST http://localhost:8000/run-provider-experiment \
  -H "Content-Type: application/json" \
  -d '{
    "config": {
      "provider": "openai",
      "model_name": "gpt-5-mini",
      "api_key": "YOUR_API_KEY",
      "temperature": 0.1,
      "max_tokens": 1024,
      "system_prompt": ""
    }
  }'
```
