from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.bash.operator import BashOperator
from src.logs import log_pipeline


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
    start_date=datetime(2026,6,29),
    catchup=False,
    tags=['meteorologia', 'pyspark', 'dbt', 'ml'],
)

def main_pipeline():
    gerenciador=log()
    with gerenciador.log_pipeline("main_pipeline") as logger:
        print("começãndo pipeline")
        ingestion_job= BashOperator(
            task_id='ingestion_job',
            bash_command='python3 main.py',
        )
        dbt_job= BashOperator(
            task_id='dbt_job',
            bash_command='dbt run ',
        )
        dbt_test_job= BashOperator(
            task_id='dbt_test_job',
            bash_command='dbt test ',
        )
        