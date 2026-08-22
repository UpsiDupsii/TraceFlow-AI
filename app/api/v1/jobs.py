import uuid
from fastapi import APIRouter, status, HTTPException
from app.schemas.job import JobCreate, JobResponse
from app.services.llm import llm_service
from app.core.logging import logger

router = APIRouter(prefix="/jobs", tags=["Jobs"])

@router.post(
    "",
    response_model=JobResponse,
    status_code=status.HTTP_200_OK,
    summary="Execute an LLM job synchronously (Phase 1 Baseline)"
)
async def create_job(payload: JobCreate):
    job_id = f"job_{uuid.uuid4().hex[:12]}"
    
    logger.info(
        f"Processing Job | job_id={job_id} | provider={payload.provider} | model={payload.model_name}"
    )
    
    try:
        # Step 1: Direct Execution via Ollama Service
        result = await llm_service.generate_response(
            prompt=payload.prompt,
            model_name=payload.model_name
        )
        
        return JobResponse(
            job_id=job_id,
            status="COMPLETED",
            provider=payload.provider,
            model_name=payload.model_name,
            output=result["response"],
            latency_ms=result["execution_time_ms"],
            message="Job executed successfully via local Ollama"
        )
    except Exception as e:
        logger.error(f"Job execution failed | job_id={job_id} | error={str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"LLM execution error: {str(e)}"
        )