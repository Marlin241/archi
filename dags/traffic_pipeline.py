from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess
import os

# Variables réseau Docker — injectées via docker-compose environment
KAFKA_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
ES_URL = os.getenv('ELASTICSEARCH_URL', 'http://elasticsearch:9200')
SCRIPTS_DIR = '/opt/airflow/scripts'


def run_produce():
    env = {**os.environ, 'KAFKA_BOOTSTRAP_SERVERS': KAFKA_SERVERS}
    result = subprocess.run(
        ["python", "produce_traffic.py"],
        capture_output=True, text=True, timeout=120,
        cwd=SCRIPTS_DIR, env=env
    )
    print("STDOUT:", result.stdout)
    if result.stderr:
        print("STDERR:", result.stderr)
    result.check_returncode()


def run_consume():
    env = {
        **os.environ,
        'KAFKA_BOOTSTRAP_SERVERS': KAFKA_SERVERS,
        'ELASTICSEARCH_URL': ES_URL,
        'MAX_MESSAGES': '1000',  # s'arrête après avoir consommé les 1000 messages du producer
    }
    result = subprocess.run(
        ["python", "consume_traffic.py"],
        capture_output=True, text=True, timeout=300,
        cwd=SCRIPTS_DIR, env=env
    )
    print("STDOUT:", result.stdout)
    if result.stderr:
        print("STDERR:", result.stderr)
    result.check_returncode()


with DAG(
    'smart_city_final_v3',
    start_date=datetime(2026, 5, 11),
    schedule_interval='@hourly',
    catchup=False,
    tags=['smart-city']
) as dag:

    produce = PythonOperator(
        task_id='produce_traffic',
        python_callable=run_produce
    )

    consume = PythonOperator(
        task_id='consume_to_es',
        python_callable=run_consume
    )

    produce >> consume