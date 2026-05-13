from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import subprocess
import os

def install_packages():
    subprocess.check_call([sys.executable, "-m", "pip", "install", "kafka-python", "requests"])

with DAG(
    'smart_city_final_v3',
    start_date=datetime(2026, 5, 11),
    schedule_interval='@hourly',
    catchup=False,
    tags=['smart-city']
) as dag:

    install = PythonOperator(
        task_id='install_packages',
        python_callable=install_packages
    )

    produce = BashOperator(
        task_id='produce_traffic',
        bash_command='docker-compose run --rm --no-deps airflow-producer'
    )

    def run_consume():
        os.chdir('/opt/airflow/scripts')
        result = subprocess.run(["python", "consume_traffic.py"], 
                              capture_output=True, text=True, timeout=120)
        print("STDOUT:", result.stdout)
        if result.stderr:
            print("STDERR:", result.stderr)
        result.check_returncode()

    consume = PythonOperator(
        task_id='consume_to_es',
        python_callable=run_consume
    )

    install >> produce >> consume