import os 
import boto3
from dotenv import load_dotenv
import logging as log 
from pathlib import Path

logger = log.getLogger(__name__)
load_dotenv()

s3 = boto3.client(
    's3',
    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name = os.getenv("AWS_DEFAULT_REGION")
)

bucket = os.getenv("BUCKET")

def upload_s3(local_path: Path, s3_path: str) -> None:
    try:
        s3.upload_file(str(local_path), bucket, s3_path)
        logger.info(f"Upload realizado: {s3_path}")
        
    except Exception as e:
        logger.error(f"Erro ao subir {s3_path}: {e}")
        raise