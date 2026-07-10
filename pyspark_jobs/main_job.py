from pyspark.sql import SparkSession
import os
from pyspark_jobs.jobs.kmeans_job import job_kmeans
from pyspark_jobs.jobs.job_ml_nivel_de_alerta_train import job_ml_nivel_de_alerta_train
from pyspark_jobs.jobs.job_kmeans_train import job_kmeans_train
def main_job():
   
    airflow_id = os.environ.get('AIRFLOW_RUN_ID')
    spark = SparkSession.builder \
        .appName("KMeans_Clustering") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.2") \
        .config("spark.sql.shuffle.partitions", "10") \
        .getOrCreate()

    URL_SUPABASE =os.getenv("SUPABASE_POSTGRESQL_URL")
    PROPRIEDADES = {
        "user": "postgres",
        "password": os.getenv("SUPABASE_PASSWORD"),
        "driver": "org.postgresql.Driver"}


    try:
        job_kmeans_train(URL_SUPABASE, PROPRIEDADES, spark,True,airflow_id)
        job_kmeans(URL_SUPABASE, PROPRIEDADES, spark,True,airflow_id)
        job_ml_nivel_de_alerta_train(URL_SUPABASE, PROPRIEDADES, spark,True,airflow_id)
    except Exception as e:
            print(f"Erro ao executar o processo de clustering KMeans: {e}")
            raise e
    spark.stop()