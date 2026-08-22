from enum import Enum
from typing import Any, Dict, Optional
from pydantic import BaseModel, Field
from app.core.config import settings

class ModelProvider(str, Enum):
    OLLAMA = "ollama"
    OPENAI = "openai"
    ANTHROPIC = "anthropic"

class JobCreate(BaseModel):
    provider: ModelProvider = Field(default=ModelProvider.OLLAMA, description="Target LLM provider")
    model_name: str = Field(default=settings.DEFAULT_MODEL, description="LLM model name")
    prompt: str = Field(..., min_length=1, description="Prompt payload to execute")
    parameters: Optional[Dict[str, Any]] = Field(default_factory=dict)

class JobResponse(BaseModel):
    job_id: str
    status: str
    provider: ModelProvider
    model_name: str
    output: Optional[str] = None
    latency_ms: Optional[float] = None
    message: str
    