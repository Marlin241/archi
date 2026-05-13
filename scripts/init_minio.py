import os
import time
import requests
from minio import Minio
from minio.error import S3Error

ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
BUCKETS = [b.strip() for b in os.getenv("MINIO_BUCKETS", "traffic-bucket").split(",") if b.strip()]

health_url = f"http://{ENDPOINT}/minio/health/live"
for i in range(30):
    try:
        if requests.get(health_url, timeout=3).status_code == 200:
            print("MinIO prêt.")
            break
    except Exception:
        pass
    print(f"Attente de MinIO... ({i + 1}/30)")
    time.sleep(3)

client = Minio(ENDPOINT, access_key=USER, secret_key=PASSWORD, secure=False)

for bucket in BUCKETS:
    try:
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
            print(f"[CRÉÉ]     {bucket}")
        else:
            print(f"[EXISTANT] {bucket}")
    except S3Error as e:
        print(f"[ERREUR]   {bucket}: {e}")
        raise
