from fastapi import APIRouter, HTTPException, Depends
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
import hmac
import hashlib
import json
from datetime import datetime, timedelta
from typing import Dict, Any
from urllib.parse import parse_qsl
import jwt
from pydantic import BaseModel
from .settings import settings

router = APIRouter()

class InitDataIn(BaseModel):
    init_data: str

_bearer = HTTPBearer(auto_error=False)


def _check_init_data(init_data: str) -> Dict[str, str]:
    params = dict(parse_qsl(init_data, keep_blank_values=True))
    hash_val = params.get("hash")
    if not hash_val:
        raise HTTPException(status_code=400, detail="missing hash")
    secret = settings.telegram_webapp_secret
    if not secret:
        raise HTTPException(status_code=500, detail="telegram secret not configured")
    data_pairs = [f"{k}={v}" for k, v in sorted(params.items()) if k != "hash"]
    data_check_string = "\n".join(data_pairs)
    calc_hash = hmac.new(secret.encode(), data_check_string.encode(), hashlib.sha256).hexdigest()
    if calc_hash != hash_val:
        raise HTTPException(status_code=403, detail="invalid signature")
    return params


def create_jwt(payload: Dict[str, Any], expires_in: int = 300) -> str:
    to_encode = payload.copy()
    to_encode["exp"] = datetime.utcnow() + timedelta(seconds=expires_in)
    return jwt.encode(to_encode, settings.jwt_secret, algorithm="HS256")


def verify_jwt(
    creds: HTTPAuthorizationCredentials = Depends(_bearer),
) -> Dict[str, Any]:
    if creds is None:
        raise HTTPException(status_code=401, detail="missing authorization")
    token = creds.credentials
    try:
        return jwt.decode(token, settings.jwt_secret, algorithms=["HS256"])
    except jwt.PyJWTError:
        raise HTTPException(status_code=401, detail="invalid token")


@router.post("/auth/telegram_webapp")
def auth_telegram_webapp(payload: InitDataIn) -> Dict[str, Any]:
    params = _check_init_data(payload.init_data)
    user_raw = params.get("user")
    profile = json.loads(user_raw) if user_raw else {}
    token = create_jwt({"user": profile})
    return {"token": token, "profile": profile}
