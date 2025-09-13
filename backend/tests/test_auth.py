import json
import json
import hmac
import hashlib
from urllib.parse import urlencode

import os
import jwt
from fastapi.testclient import TestClient


def make_init_data(user, secret):
    params = {
        "auth_date": "111111",
        "query_id": "test",
        "user": json.dumps(user),
    }
    data_check_string = "\n".join(
        f"{k}={params[k]}" for k in sorted(params)
    )
    params["hash"] = hmac.new(
        secret.encode(), data_check_string.encode(), hashlib.sha256
    ).hexdigest()
    return urlencode(params)


def test_telegram_auth_valid_and_invalid(monkeypatch):
    monkeypatch.setenv("SUPABASE_URL", "http://example.com")
    monkeypatch.setenv("SUPABASE_SERVICE_ROLE", "dummy")
    from app.settings import settings
    monkeypatch.setattr(settings, "telegram_webapp_secret", "webapp_secret")
    monkeypatch.setattr(settings, "jwt_secret", "jwt_secret")
    from app.main import app
    client = TestClient(app)

    user = {"id": 1, "first_name": "Alice"}
    init_data = make_init_data(user, "webapp_secret")
    res = client.post("/auth/telegram_webapp", json={"init_data": init_data})
    assert res.status_code == 200
    data = res.json()
    assert data["profile"]["id"] == 1
    # token decodes
    decoded = jwt.decode(data["token"], "jwt_secret", algorithms=["HS256"])
    assert decoded["user"]["id"] == 1

    bad_params = {
        "auth_date": "111111",
        "query_id": "test",
        "user": json.dumps(user),
        "hash": "deadbeef",
    }
    bad_init = urlencode(bad_params)
    res_bad = client.post("/auth/telegram_webapp", json={"init_data": bad_init})
    assert res_bad.status_code == 403
