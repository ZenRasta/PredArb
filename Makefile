.PHONY: help redis-up redis-down api worker webapp-dev

help:
	@echo "Targets:"
	@echo "  redis-up     - start Redis (docker-compose)"
	@echo "  redis-down   - stop Redis"
	@echo "  api          - run FastAPI locally"
	@echo "  worker       - run Celery worker locally"
	@echo "  webapp-dev   - run webapp dev server"

redis-up:
	docker compose -f infra/docker/docker-compose.yml up -d redis

redis-down:
	docker compose -f infra/docker/docker-compose.yml down

api:
	cd backend && \
	python -m venv .venv && . .venv/bin/activate && \
	pip install -U pip && pip install -e . && \
	uvicorn app.main:app --host 0.0.0.0 --port 8080 --reload

worker:
	cd backend && \
	. .venv/bin/activate && \
	celery -A app.celery_app.celery worker -l info

webapp-dev:
	cd webapp && pnpm install && pnpm dev
