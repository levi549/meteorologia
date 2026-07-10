from datetime import datetime, timedelta
from airflow.decorators import dag, task
from src.logs import log_pipeline
from pyspark_jobs.main_job import main_job
from  pysaprk_jobs.kmeans_train_job import kmeans_train_job
import sys
import os
from airflow.providers.docker.operators.docker import DockerOperator

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
default_args={
    'owner':'meteorologia',
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
}
@dag(
    dag_id='pipeline_meteorologia_main',
    default_args=default_args,
    description='orquestração da pipeline principal de meteorologia',
    schedule_interval='@once',
    start_date=datetime(2026,7,1),
    catchup=False,
    on_failure_callback=log_pipeline.log_erro,
    on_success_callback=log_pipeline.log_sucesso,
    tags=['meteorologia', 'pyspark', 'dbt', 'ml'],
)

def main_pipeline():


    @task(task_id="log_pipeline_inicio")
    def log_pipeline_inicio(airflow_id):
        logger = log_pipeline()
        logger.log_inicio(nome_pipeline='pipeline_meteorologia_main', airflow_id=airflow_id)

    ingestion_job= BashOperator(
        task_id='ingestion_job',
        bash_command=' uv run python main.py',
    )
    dbt_job= BashOperator(
        task_id='dbt_job',
        bash_command='dbt run ',
    )
    dbt_test_job= BashOperator(
        task_id='dbt_test_job',
        bash_command='dbt test ',
    )
    @task(task_id="main_job")
    def run_main_job(airflow_id):
        main_job(airflow_id)

id_pipeline = '{{ run_id }}'

log_pipeline_inicio(id_pipeline) >> ingestion_job >> dbt_job >> dbt_test_job >> run_main_job(id_pipeline)

pipeline_main_dag = main_pipeline()

