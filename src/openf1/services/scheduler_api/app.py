from enum import StrEnum
import os
import json
import logging
from fastapi import FastAPI, HTTPException, Response
from pydantic import BaseModel
from dapr.clients import DaprClient, Job

from openf1.services.ingestor_livetiming.historical.main import ingest_meeting

# Add protobuf availability check
try:
    from google.protobuf.any_pb2 import Any as GrpcAny
    PROTOBUF_AVAILABLE = True
except ImportError:
    PROTOBUF_AVAILABLE = False
    print('Warning: protobuf not available, jobs with data will be scheduled without data', flush=True)


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI()

# Get app port from environment
app_port = int(os.getenv('APP_PORT', '6200'))

# Pydantic models for request/response
class IngestService(StrEnum):
    INGEST_MEETING = "ingest-meeting"


class IngestJobData(BaseModel):
    service: IngestService
    year: int | None = None
    meeting_key: int | None = None
    session_key: int | None = None
    collection_names: list[str] | None = None


class IngestJob(BaseModel):
    name: str
    data: IngestJobData
    due_time: str


def create_job_data(data_dict: dict):
    # Create job data from a dictionary
    if not PROTOBUF_AVAILABLE:
        return None

    data = GrpcAny()
    data.value = json.dumps(data_dict).encode('utf-8')
    return data


@app.post("/scheduleJob")
def schedule_job(job: IngestJob, response: Response):

    print(f"Scheduling job: {job.name}", flush=True)

    try:
        # Create the job
        job = Job(
            name=job.name,
            due_time=f"{job.due_time}",
            data=create_job_data(job.data.model_dump())
        )
        with DaprClient() as d:
            # Schedule the job
            d.schedule_job_alpha1(job=job, overwrite=True)

        print(f"Job scheduled: {job.name}", flush=True)

        # Set 200 status and return the payload
        response.status_code = 200
        return job

    except Exception as e:
        print(f"Error scheduling job: {e}")
        raise HTTPException(
            status_code=500, detail=f"Error scheduling job: {str(e)}")


@app.get("/getJob/{name}")
async def get_job(name: str):

    print(f"Retrieving job: {name}")

    try:
        with DaprClient() as d:
            job = d.get_job_alpha1(name)

        if job is None:
            raise HTTPException(status_code=404, detail="Job not found")

        # Convert protobuf job object to dict for JSON serialization
        job_dict = {
            "name": job.name,
            "due_time": job.due_time,
        }

        # Handle job data if present
        if job.data:
            try:
                payload = json.loads(job.data.value.decode('utf-8'))
                job_dict["data"] = payload
            except Exception:
                job_dict["data"] = f"<binary data, {len(job.data.value)} bytes>"
        else:
            job_dict["data"] = None

        return job_dict

    except Exception as e:
        print(f"Error getting job: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@app.delete("/deleteJob/{name}")
async def delete_job(name: str):
    print(f"Deleting job: {name}")

    try:
        with DaprClient() as d:
            job_details = d.delete_job_alpha1(name)
        print(f"Job deleted: {name}")
        return {"message": "Job deleted"}

    except Exception as e:
        print(f"Error deleting job: {e}")
        raise HTTPException(status_code=400, detail="Error deleting job")


@app.post("/job/{job_name}")
async def handle_job(job_name: str, job_data: IngestJobData):

    try:
        # Extract job data from payload
        match job_data.service:
            case IngestService.INGEST_MEETING:
                await ingest_meeting(
                    year=job_data.year,
                    meeting_key=job_data.meeting_key,
                    collection_names=job_data.collection_names,
                )
            case _:
                raise HTTPException(
                    status_code=500, detail=f"No valid service"
                )
            
        return {"status": "success", "name": job_name}
    
    except Exception as ex:
        print(f"Failed to handle job {job_name}")
        print(f"Error handling job: {ex}")
        raise HTTPException(
            status_code=500, detail=f"Error handling job: {str(ex)}"
        )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=app_port)
