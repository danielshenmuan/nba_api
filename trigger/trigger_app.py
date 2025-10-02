from fastapi import FastAPI
from google.cloud import run_v2
import os

app = FastAPI()

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/trigger")
def trigger_job():
    """
    Trigger the nba-daily-ingest Cloud Run job via API.
    """
    project_id = os.getenv("PROJECT_ID", "fantasy-survivor-app")
    location = os.getenv("REGION", "us-central1")
    job_name = os.getenv("JOB_NAME", "nba-daily-ingest")

    client = run_v2.JobsClient()
    job_path = f"projects/{project_id}/locations/{location}/jobs/{job_name}"

    operation = client.run_job(name=job_path)
    response = operation.result()

    return {"status": "success", "job": job_name, "execution": response.name}
