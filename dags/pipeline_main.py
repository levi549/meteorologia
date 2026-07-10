from datetime import datetime, timedelta
import os
import sys
from airflow.decorators import dag, task
from airflow.providers.docker.operators.docker import DockerOperator
from src.logs import log_pipeline

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

default_args = {
    'owner': 'meteorologia',
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
}

@dag(
    dag_id='pipeline_meteorologia_main',
    default_args=default_args,
    description='orquestração da pipeline principal de meteorologia via Docker',
    schedule_interval='@once',
    start_date=datetime(2026, 7, 8),
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

   
    ingestion_job = DockerOperator(
        task_id='ingestion_job',
        image='meteorologia-ingestion:latest',
        command='uv run python main.py',
        network_mode='meteorologia_network',
        dns=['8.8.8.8', '1.1.1.1'],
        auto_remove=True,
        api_version='auto'
    )

   
    dbt_job = DockerOperator(
        task_id='dbt_job',
        image='meteorologia-dbt:latest',
        command='dbt run',
        network_mode='meteorologia_network',
        dns=['8.8.8.8', '1.1.1.1'],
        auto_remove=True,
        api_version='auto'
    )

   
    dbt_test_job = DockerOperator(
        task_id='dbt_test_job',
        image='meteorologia-dbt:latest',
        command='dbt test',
        network_mode='meteorologia_network',
        dns=['8.8.8.8', '1.1.1.1'],
        auto_remove=True,
        api_version='auto'
    )

    
    run_main_job = DockerOperator(
        task_id='main_job',
        image='meteorologia-pyspark:latest',
        command='python /app/pyspark_jobs/main_job.py',
        network_mode='meteorologia_network',
        dns=['8.8.8.8', '1.1.1.1'],
        auto_remove=True,
        api_version='auto',
        environment={
            'AIRFLOW_RUN_ID': '{{ run_id }}'
        }
    )

   
    id_pipeline = '{{ run_id }}'
    
    log_pipeline_inicio(id_pipeline) >> ingestion_job >> dbt_job >> dbt_test_job >> run_main_job

pipeline_main_dag = main_pipeline()