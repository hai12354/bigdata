from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'bigdata_team',
    'start_date': days_ago(1),
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

# Gọi producer đẩy 1 message nhanh gọn mỗi lần trigger
# Sửa bash_command trong dags/news_pipeline_dag.py
task_run_producer = BashOperator(
    task_id='fetch_2apis_and_push_kafka_loop',
    bash_command='for i in $(seq 1 10); do python3 /opt/airflow/scripts/producer_2apis.py once; sleep 0.5; done',
    dag=dag
)