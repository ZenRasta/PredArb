PredArb Monorepo
=================

Goal: A runnable monorepo skeleton including:
- FastAPI backend + Celery worker
- React Telegram WebApp
- Supabase SQL schema (checked in)
- Redis for development via Docker
- DigitalOcean App spec and CI stubs
- Shared env vars and dev scripts wired

Architecture
------------

See [docs/architecture.md](docs/architecture.md) for a high-level diagram.

Components:

- **Backend**: FastAPI service providing the API.
- **Celery workers**: asynchronous task executors used by the backend.
- **Supabase**: Postgres database and authentication layer.
- **Redis**: cache and message broker connecting the backend and workers.
- **Bot**: Telegram bot server interfacing with the backend and webapp.
- **Webapp**: React-based Telegram WebApp frontend.

Quick Start
-----------

1) Prereqs
- Python 3.10+
- Node 18+
- pnpm (preferred) or npm
- Docker + Docker Compose
- Git

2) Clone and prepare
```bash
cd predarb
cp .env.example .env
```

3) Dev: Redis (Docker)
```bash
docker compose -f infra/docker/docker-compose.yml up -d redis
```

4) Dev: Backend API
```bash
cd backend
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload
```

5) Dev: Celery worker
```bash
cd backend
source .venv/bin/activate
celery -A app.celery_app.app worker -l info
```

6) Dev: WebApp (React)
```bash
cd webapp
pnpm install # or: npm install
pnpm dev     # or: npm run dev
```

Environments
------------

- Shared variables live in `/.env.example`. Service-specific examples exist under each service.
- Supabase schema SQL lives under `infra/sql/schema.sql`.
- DigitalOcean App Platform spec is under `infra/do/app.yaml`.

Repo Layout
-----------

```
predarb/
  backend/        # FastAPI + Celery skeleton
  bot/            # Reserved for future Telegram bot server
  webapp/         # React Telegram WebApp (Vite)
  infra/
    sql/          # Supabase schema
    docker/       # docker-compose (redis)
    ci/           # CI workflows
    do/           # DigitalOcean app spec
  docs/
```

