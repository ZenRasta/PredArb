# PredArb Backend (FastAPI + Celery)

Run locally
----------

```bash
cp .env.example .env
python -m venv .venv && source .venv/bin/activate
pip install -U pip && pip install -e .
uvicorn app.main:app --reload --port 8080
```

Celery worker
-------------

```bash
source .venv/bin/activate
celery -A app.celery_app.celery worker -l info
```

Test tasks in a Python shell:

```python
from app.tasks import ping, slow_add
ping.delay()          # -> AsyncResult
slow_add.delay(2, 3)  # -> 5
```
