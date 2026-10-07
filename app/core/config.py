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
    
    # INFRASTRUCTURE CONFIGURATIONS
    KAFKA_BOOTSTRAP_SERVERS: str
    REDIS_URL: str
    KAFKA_TOPIC_JOBS: str = "llm_jobs"
    KAFKA_TOPIC_DLQ: str = "llm_jobs_dlq"
    MAX_RETRIES: int = 3
    RETRY_BASE_DELAY_SEC: float = 1.0

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

settings = Settings()