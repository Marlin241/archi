from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 3),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'check_smart_city_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    check_kafka = BashOperator(
        task_id='check_kafka_connection',
        bash_command='echo "Kafka est prêt à recevoir des données !"'
    )

    process_data = BashOperator(
        task_id='trigger_spark_job',
        bash_command='echo "Le script Spark sera lancé ici via spark-submit"'
    )

    check_kafka >> process_data