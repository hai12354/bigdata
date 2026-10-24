from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'bigdata_team',
    'start_date': datetime(2026, 10, 1),
    'retries': 1,
    'retry_delay': timedelta(seconds=10)
}

dag = DAG(
    'news_kafka_pipeline',
    default_args=default_args,
    description='Airflow DAG Trigger gọi Producer đẩy dữ liệu vào Kafka',
    schedule_interval='* * * * *',
    catchup=False,
    max_active_runs=1
)

# Gọi đúng tên file producer_2apis.py
task_run_producer = BashOperator(
    task_id='fetch_2apis_and_push_kafka_loop',
    bash_command='for i in $(seq 1 12); do python3 /opt/airflow/scripts/producer_2apis.py once; sleep 5; done',
    dag=dag
)