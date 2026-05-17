from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    database_url: str
    redis_url: str
    api_refresh_seconds: int = 2

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = Settings()
