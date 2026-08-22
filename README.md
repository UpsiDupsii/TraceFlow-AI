# TraceFlow-AI

An event-driven, distributed API gateway and execution engine for distributed AI workflows, prompt tracing, and async LLM task queue management.

Designed to eliminate LLM processing bottlenecks, handle rate limits gracefully, and provide complete observability over distributed AI execution pipelines.

---

## Architecture Blueprint


```

[ Client / Postman ] ──► [ FastAPI Gateway ] ──► [ Ollama Service (qwen2.5:3b) ]

```

---

## Phase 1 Implementation

- **Fail-Fast Core Engine:** Built with FastAPI and `pydantic-settings` to enforce strict environment configuration without runtime fallbacks.
- **Async LLM Service:** Integrated with local Ollama (`qwen2.5:3b`) using non-blocking async execution.
- **Contract Enforcement:** Pydantic v2 schemas validating inbound payloads and outbound metadata contracts.
- **Structured Telemetry:** Standardized log streams for tracing job execution lifecycles.

### Benchmark Metrics
- **Execution Mode:** Synchronous (Request/Response)
- **Target Model:** `qwen2.5:3b`
- **Average Latency:** `~15.2s`

---

## Quickstart

### Prerequisites
- Python 3.12+
- Ollama running locally (`ollama pull qwen2.5:3b`)

### Setup & Run
```bash
# Configure environment
cp .env.example .env

# Create virtual environment & install dependencies
python -m venv venv
source venv/bin/activate  # On WSL / Linux
pip install -r requirements.txt

# Start API server
uvicorn app.main:app --reload --port 8000

```

### API Endpoints

| Method | Endpoint | Description | Status |
| --- | --- | --- | --- |
| `GET` | `/health` | Service health & version assertion | `200 OK` |
| `POST` | `/api/v1/jobs` | Submit LLM generation job | `200 OK` |
