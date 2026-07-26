import sys
import os
import shutil
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.ML import ML_kmeans
from pyspark.sql import functions as F
from pyspark.ml.feature import  VectorAssembler
from pyspark.sql.window import Window
import os
from src.logs import log
from src.predicate import Predicate
import pyscopg2
def job_kmeans(URL_SUPABASE, PROPRIEDADES, spark,write_parfquet=None,id=None):
    gerenciador=log()
    with gerenciador.log_job("kmeans_job",id) as logger:
        print("Iniciando o processo de clustering KMeans")

        caminho_atual = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
        caminho_parquet = os.path.join(caminho_atual, "parquet")
        caminho_modelo = os.path.join(caminho_atual, "modelos/kmeans")
        limites = spark.read.jdbc(
            url=URL_SUPABASE,
            table="""(SELECT (
                SELECT COALESCE(MAX(ultima_dt_processada), '1970-01-01 00:00:00+00'::timestamptz) 
                FROM public.log_job
                WHERE nome_job = 'kmeans_job' 
                AND status = 'SUCCESS'
            ) as min_dt, MAX(ingested_at) as max_dt,
            MAX(dt) as max_dt2, MIN(dt) as min_dt2
            FROM public.gold_dados_kmeans) as limites""",
            properties=PROPRIEDADES
        ).first()
            
        v_min = limites["min_dt"] 
        v_max = limites["max_dt"]
        v_min2 = limites["min_dt2"]
        v_max2 = limites["max_dt2"]
        predicates=Predicate(v_min, v_max, v_min2, v_max2,10).gerar_predicate()
        df=spark.read.jdbc( 
        url=URL_SUPABASE,
        table="public.gold_dados_kmeans",
        properties=PROPRIEDADES,
        predicates=predicates
        )
            
        assembler=VectorAssembler(inputCols=[
            "mes_sin",
            "mes_cos",
            "temp_padronizado",
            "humidity_padronizado",
            "pressure_padronizada"
        ], outputCol="features")
        df_features=assembler.transform(df)
        kmeans=ML_kmeans()
        kmeans.load_model(caminho_modelo)
        df_final=kmeans.predict(df_features).select(
            'id',
            "dt",
            "anomaly_name" ,
            "mes_sin",
            "mes_cos",
            F.col("temp_padronizado").alias("temp"),
            F.col("humidity_padronizado").alias("humidity"),
            F.col("pressure_padronizada").alias("pressure"),
            "prediction").withColumnRenamed("prediction","Nivel_de_alerta").withColumn("ingested_at", F.current_timestamp())
        
        if write_parfquet:
            if os.path.exists(caminho_parquet):
                shutil.rmtree(caminho_parquet, ignore_errors=True)
            df_final.write\
            .mode("append")\
            .parquet(caminho_parquet)
            print(f"Dados do clustering KMeans gravados no formato Parquet em: {caminho_parquet}")
            df_parquet = spark.read.parquet(caminho_parquet)
            df_parquet.write.jdbc(
                url=URL_SUPABASE,   
                table="public.kmeans_resultado_staging",
                mode="overwrite",
                properties=PROPRIEDADES
            )
        else:
            df_final.write.jdbc(
                url=URL_SUPABASE,   
                table="public.kmeans_resultado_staging",
                mode="overwrite",
                properties=PROPRIEDADES
            )
        logger.ultima_dt_processada=v_max
       
        print("Processo de clustering KMeans concluído com sucesso.")  