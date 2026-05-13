from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
import os

JUPYTER = os.getenv("JUPYTER_CONTAINER_NAME", "architecturebigdata-jupyter-pyspark-1")
KAFKA = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_PASS = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
ICEBERG_BUCKET = os.getenv("MINIO_ICEBERG_BUCKET", "traffic-bucket")

SPARK_PYTHON_PATH = (
    "/usr/local/spark/python"
    ":/usr/local/spark/python/lib/py4j-0.10.9.7-src.zip"
)

with DAG(
    "spark_kafka_to_iceberg",
    start_date=datetime(2026, 5, 11),
    schedule_interval="*/15 * * * *",  # toutes les 15 min — Spark ~30-60s de démarrage
    catchup=False,
    tags=["smart-city", "spark"],
) as dag:

    run_spark = BashOperator(
        task_id="spark_kafka_to_iceberg",
        bash_command=(
            f"docker exec "
            f"-e PYTHONPATH={SPARK_PYTHON_PATH} "
            f"-e SPARK_HOME=/usr/local/spark "
            f"-e KAFKA_BOOTSTRAP_SERVERS={KAFKA} "
            f"-e MINIO_ROOT_USER={MINIO_USER} "
            f"-e MINIO_ROOT_PASSWORD={MINIO_PASS} "
            f"-e MINIO_ENDPOINT=http://minio:9000 "
            f"-e MINIO_ICEBERG_BUCKET={ICEBERG_BUCKET} "
            f"{JUPYTER} "
            f"python /home/jovyan/scripts/spark_kafka_to_iceberg.py"
        ),
    )
