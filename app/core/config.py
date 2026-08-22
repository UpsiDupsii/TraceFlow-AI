from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    # PROJECT CONFIGURATION
    PROJECT_NAME: str
    VERSION: str
    API_V1_STR: str
    ENVIRONMENT: str

    # LLM CONFIGURATIONS
    OLLAMA_BASE_URL: str
    DEFAULT_MODEL: str

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

settings = Settings()