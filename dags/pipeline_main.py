from datetime import datetime, timedelta
import os
import sys
from airflow.decorators import dag, task
from airflow.providers.docker.operators.docker import DockerOperator
sys.path.append('/opt/airflow')
from src.logs import log_pipeline
from docker.types import Mount

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
log=log_pipeline()
default_args = {
    'owner': 'meteorologia',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

@dag(
    dag_id='pipeline_meteorologia_main',
    default_args=default_args,
    description='orquestração da pipeline principal de meteorologia via Docker',
    schedule_interval='@once',
    start_date=datetime(2026, 7, 26),
    catchup=False,
    on_failure_callback=log.log_erro,
    on_success_callback=log.log_sucesso,
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
        command='python main.py',
        network_mode='minha-rede',
        dns=['8.8.8.8', '1.1.1.1'],
        auto_remove=True,
        mount_tmp_dir=False,
        api_version='auto',
        environment={
            'AIRFLOW_RUN_ID': '{{ run_id }}',
            'PYTHONPATH': '/app',
            'SUPABASE_URL': os.getenv("SUPABASE_URL"),
            'SUPABASE_KEY': os.getenv("SUPABASE_KEY"),
            'SIDRA_API':os.getenv("SIDRA_API"),
            'IBGE_API':os.getenv("IBGE_API")
        }
    )

   
    dbt_job = DockerOperator(
        task_id='dbt_job',
        image='meteorologia-dbt:latest',
        command='dbt run',
        network_mode='minha-rede',
        dns=['8.8.8.8', '1.1.1.1'],
        auto_remove=True,
        api_version='auto'
    )

   
    dbt_test_job = DockerOperator(
        task_id='dbt_test_job',
        image='meteorologia-dbt:latest',
        command='dbt test',
        network_mode='minha-rede',
        dns=['8.8.8.8', '1.1.1.1'],
        auto_remove=True,
        api_version='auto'
    )

    
    run_main_job = DockerOperator(
        task_id='main_job',
        image='meteorologia-pyspark:latest',
        command='python /app/pyspark_jobs/main_job.py',
        network_mode='minha-rede', 
        dns=['8.8.8.8', '1.1.1.1'],
        mount_tmp_dir=False,
        auto_remove=True,
        api_version='auto',
        environment={
            'AIRFLOW_RUN_ID': '{{ run_id }}',
            'SUPABASE_URL': os.getenv("SUPABASE_URL"),
            'SUPABASE_KEY': os.getenv("SUPABASE_KEY"),
            'SUPABASE_PASSWORD': os.getenv("SUPABASE_PASSWORD"),
            'SUPABASE_POSTGRESQL_URL': os.getenv("SUPABASE_POSTGRESQL_URL")
        },
        mounts=[
            Mount(
                source='C:/Users/carta/Desktop/Meteorologia/parquet', 
                target='/app/parquet', 
                type='bind'
            ),
            Mount(
                source='C:/Users/carta/Desktop/Meteorologia/modelos', 
                target='/app/modelos', 
                type='bind'
            )
        ]
    )

   
    id_pipeline = '{{ run_id }}'
    
    log_pipeline_inicio(id_pipeline)>> ingestion_job >> dbt_job >> dbt_test_job >> run_main_job

pipeline_main_dag = main_pipeline() 
