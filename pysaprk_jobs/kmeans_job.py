from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import  VectorAssembler
import os
from src.ML import ML
from pyspark.sql.window import Window
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

def job_kmeans():
    try:
        print("Iniciando o processo de clustering KMeans")
        
        limites = spark.read.jdbc(
            url=URL_SUPABASE,
            table="(SELECT MIN(id) as min_id, MAX(id) as max_id FROM public.dados_csv_silver) as limites",
            properties=PROPRIEDADES
        ).collect()[0]
        
        v_min = limites["min_id"] 
        v_max = limites["max_id"] 

        df=spark.read.jdbc(
        url=URL_SUPABASE,
        table="public.dados_csv_silver",
        column="id",
        lowerBound=v_min,
        upperBound=v_max,
        numPartitions=10,
        properties=PROPRIEDADES
    )
        janela=Window.partitionBy('city_id')
        df = df.dropna()
        df_padronizado=df.withColumn("temp_padronizado", F.when(F.stddev("temp").over(janela) == 0,0.0)\
            .otherwise((F.col("temp") - F.mean("temp").over(janela)) / F.stddev("temp").over(janela))) \
            .withColumn("humidity_padronizado", F.when(F.stddev("humidity").over(janela) == 0,0.0)\
            .otherwise((F.col("humidity") - F.mean("humidity").over(janela)) / F.stddev("humidity").over(janela))) \
            .withColumn("pressure_padronizado", F.when(F.stddev("pressure").over(janela) == 0,0.0)\
            .otherwise((F.col("pressure") - F.mean("pressure").over(janela)) / F.stddev("pressure").over(janela)))
        
        assembler=VectorAssembler(inputCols=[
            "mes_sin",
            "mes_cos",
            "temp_padronizado",
            "humidity_padronizado",
            "pressure_padronizado"
        ], outputCol="features")
        df_features=assembler.transform(df_padronizado)
        kmeans=ML()
        kmeans.treino(df_features)
        df_final=kmeans.resultado.select(
            'id',
            "anomaly_name",
            "mes_sin",
            "mes_cos",
            "temp",
            "humidity",
            "pressure",
            "prediction").withColumn(
                "Nivel_de_risco", F.when(F.col("prediction") == 0, "Baixo") \
                 .when(F.col("prediction") == 1, "Médio") \
                 .otherwise("Alto")
                ).drop("prediction")

        df_final.write.jdbc(
            url=URL_SUPABASE,
            table="public.kmeans_resultado",
            mode="overwrite",
            properties=PROPRIEDADES
        )
        print("Processo de clustering KMeans concluído com sucesso.")

    except Exception as e:
        print(f"Erro ao executar o processo de clustering KMeans: {e}")
        raise e
def job_ml_supervisionado():
    pass
def main():
    try:
        job_kmeans()
        job_ml_supervisionado()
    except Exception as e:
        print(f"Erro ao executar o processo de clustering KMeans: {e}")
        raise e