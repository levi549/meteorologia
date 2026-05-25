from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import  VectorAssembler
import os
from src.ML import ML
spark = SparkSession.builder \
    .appName("KMeans_Clustering") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.2") \
    .getOrCreate()

URL_SUPABASE =os.getenv("SUPABASE_POSTGRESQL_URL")
PROPRIEDADES = {
    "user": "postgres",
    "password": os.getenv("SUPABASE_PASSWORD"),
    "driver": "org.postgresql.Driver"}


def main():
    try:
        print("Iniciando o processo de clustering KMeans")
        
        df=spark.read.jdbc(
        url=URL_SUPABASE,
        table="public.dados_csv_silver",
        properties=PROPRIEDADES
    )



        assembler=VectorAssembler(inputCols=[
            "mes_sin",
            "mes_cos",
            "temp_padronizado",
            "humidity_padronizado",
            "pressure_padronizado"
        ], outputCol="features")
        df_features=assembler.transform(df)
        kmeans=ML()
        kmeans.treino(df_features)
        df_final=kmeans.resultado.select(
            "id", 
            "mes_sin",
            "mes_cos",
            "temp_padronizado",
            "humidity_padronizado",
            "pressure_padronizado",
            "prediction")
        df_final.write.jdbc(
            url=URL_SUPABASE,
            table="public.kmeans_resultado",
            mode="overwrite",
            properties=PROPRIEDADES
        )


    except Exception as e:
        print(f"Erro ao executar o processo de clustering KMeans: {e}")
        raise e