from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    supabase_url: str = Field(..., alias="SUPABASE_URL")
    supabase_service_role: str = Field(..., alias="SUPABASE_SERVICE_ROLE")
    supabase_anon_key: str | None = Field(None, alias="SUPABASE_ANON_KEY")
    redis_url: str = Field("redis://localhost:6379/0", alias="REDIS_URL")

    embeddings_model: str = Field("e5-large-v2", alias="EMBEDDINGS_MODEL")
    llm_model: str = Field("gpt-4o-mini", alias="LLM_MODEL")
    llm_api_key: str | None = Field(None, alias="LLM_API_KEY")

    telegram_bot_token: str | None = Field(None, alias="TELEGRAM_BOT_TOKEN")
    telegram_webapp_secret: str | None = Field(None, alias="TELEGRAM_WEBAPP_SECRET")
    jwt_secret: str = Field("dev-secret", alias="JWT_SECRET")

    class Config:
        env_file = ".env"
        extra = "ignore"


settings = Settings()  # import this across modules

