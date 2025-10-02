import os, json
from uuid import uuid4
import boto3
from fastapi import FastAPI
from mangum import Mangum
from botocore.exceptions import ClientError

BUCKET_NAME = "fastapi-bucket-project"        # p.ej. mi-lambda-jobs-bucket
QUEUE_URL   = "https://sqs.eu-north-1.amazonaws.com/423503571755/fastapi-sqs"           # URL de tu cola SQS

s3 = boto3.client("s3")
sqs = boto3.client("sqs")

app = FastAPI(title="Job Orchestrator (Lambda A)")

@app.get("/")
def root():
    return {"message": "API is running (Lambda A)"}

@app.get("/start-job")
def start_job():
    job_id = str(uuid4())

    # Estado inicial en S3
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=f"jobs/{job_id}.json",
        Body=json.dumps({"status": "running"}).encode("utf-8"),
        ContentType="application/json",
    )

    # Mensaje a SQS para que Lambda B procese
    sqs.send_message(
        QueueUrl=QUEUE_URL,
        MessageBody=json.dumps({"job_id": job_id})
    )

    return {"job_id": job_id, "status": "running"}

@app.get("/get-result/{job_id}")
def get_result(job_id: str):
    try:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=f"jobs/{job_id}.json")
        body = obj["Body"].read().decode("utf-8")
        return json.loads(body)
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            return {"error": "Job not found"}
        raise

handler = Mangum(app)

