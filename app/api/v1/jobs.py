import uuid
from fastapi import APIRouter, status, HTTPException
from app.schemas.job import JobCreate, JobAcceptedResponse, JobResponse
from app.services.kafka_producer import kafka_producer
from app.services.redis_service import redis_service
from app.core.logging import logger

router = APIRouter(prefix="/jobs", tags=["Jobs"])

@router.post(
    "",
    response_model=JobAcceptedResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Submit an LLM job asynchronously via Kafka"
)
async def create_job(payload: JobCreate):
    job_id = f"job_{uuid.uuid4().hex[:12]}"
    
    logger.info(
        f"Queuing Job | job_id={job_id} | provider={payload.provider.value} | model={payload.model_name}"
    )
    
    job_data = {
        "job_id": job_id,
        "status": "PENDING",
        "provider": payload.provider.value,
        "model_name": payload.model_name,
        "prompt": payload.prompt,
        "output": None,
        "latency_ms": None,
        "message": "Job queued for background processing."
    }
    # Save PENDING status in Redis
    await redis_service.set_job(job_id=job_id, job_data=job_data)
    
    event_payload = {
        "job_id": job_id,
        "provider": payload.provider.value,
        "model_name": payload.model_name,
        "prompt":payload.prompt,
        "parameters": payload.parameters,
        "status": "PENDING"
    }
    
    try:
        # Publish event asynchronously to Kafka topic
        await kafka_producer.send_event(topic="llm_jobs", event_data=event_payload)

        return JobAcceptedResponse(
            job_id=job_id,
            status="PENDING",
            message="Job accepted and queued for background processing."
        )
        
    except Exception as e:
        logger.error(f"Failed to queue job on Kafka | job_id={job_id} | error={str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to queue job: {str(e)}"
        )

@router.get(
    "/{job_id}",
    response_model=JobResponse,
    status_code=status.HTTP_200_OK,
    summary="Poll job status and output by ID"
)
async def get_job(job_id: str):
    job_data = await redis_service.get_job(job_id)
    if not job_data:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job with ID '{job_id}' not found or expired."
        )
    return JobResponse(**job_data)
