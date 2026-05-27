from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess

default_args = {
    "owner": "manitej",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    "youtube_data_pipeline",
    default_args=default_args,
    schedule_interval="*/5 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

def run_producer():
    subprocess.run(["python", "/opt/airflow/dags/producer.py"])



# -----------------------------
# Task 2: Run Spark Job
# -----------------------------
def run_spark():
    subprocess.run([
        "docker",
        "exec",
        "spark-master",
        "/opt/spark/bin/spark-submit",
        "--packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.6.0",
        "/opt/spark/app/spark_streaming.py"
    ], check=True)

# -----------------------------
# Tasks
# -----------------------------
kafka_task = PythonOperator(
    task_id="kafka_producer",
    python_callable=run_producer,
    dag=dag,
)

spark_task = PythonOperator(
    task_id="spark_processing",
    python_callable=run_spark,
    dag=dag,
)

# -----------------------------
# Flow
# -----------------------------
kafka_task >> spark_task