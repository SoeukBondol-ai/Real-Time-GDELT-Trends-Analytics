from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    kafka_bootstrap_servers: str
    producer_mode: str
    mock_interval_seconds: float
    gdelt_query: str
    gdelt_poll_seconds: int
    bluesky_jetstream_url: str
    hn_poll_seconds: int

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = Settings()
