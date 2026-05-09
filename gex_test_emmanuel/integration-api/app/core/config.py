from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env")

    app_name: str = "Integration API"
    app_version: str = "0.1.0"
    debug: bool = False
    grummer_aes256_key_base64: str = ""
    database_url: str = "mysql+pymysql://root:password@localhost:3306/db_integration"
    redis_url: str = "redis://localhost:6379/0"


settings = Settings()
