from src.ML import ML_kmeans
from pyspark.sql import functions as F
from pyspark.ml.feature import  VectorAssembler
from pyspark.sql.window import Window
import os
from src.logs import log
def job_kmeans(URL_SUPABASE, PROPRIEDADES, spark,write_parfquet=None,id=None):
    gerenciador=log()
    with gerenciador.log_job("kmeans_job",id) as logger:
        print("Iniciando o processo de clustering KMeans")

        caminho_atual = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
        caminho_parquet = os.path.join(caminho_atual, "parquet")

        limites = spark.read.jdbc(
            url=URL_SUPABASE,
            table="""(SELECT (
                SELECT COALESCE(MAX(ultima_dt_processada), 0) 
                FROM public.log_job
                WHERE nome_job = 'kmeans_job' 
                AND status = 'SUCCESS'
            ) as min_dt, MAX(ingested_at) as max_dt FROM public.dados_gold_kmeans) as limites""",
            properties=PROPRIEDADES
        ).first()
            
        v_min = limites["min_dt"] 
        v_max = limites["max_dt"] 

        df=spark.read.jdbc( 
        url=URL_SUPABASE,
        table=f""" (
            SELECT * FROM public.dados_gold_kmeans
            WHERE ingested_at >= '{v_min}' 
            AND ingested_at <= '{v_max}'
        ) as dados
        """,
        column="ingested_at",
        lowerBound=str(v_min),
        upperBound=str(v_max),
        numPartitions=10,
        properties=PROPRIEDADES
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
        kmeans.treino(df_features)
        df_final=kmeans.predict(df_features).select(
            'id',
            "dt",
            "anomaly_name" ,
            "mes_sin",
            "mes_cos",
            F.col("temp_padronizado").alias("temp"),
            F.col("humidity_padronizado").alias("humidity"),
            F.col("pressure_padronizada").alias("pressure"),
            "prediction").withColumnRenamed("prediction","Nivel_de_alerta")

        df_final.write.jdbc(
            url=URL_SUPABASE,   
            table="public.kmeans_resultado",
            mode="append",
            properties=PROPRIEDADES
        )
        logger.ultimo_id_processados=v_max
        if write_parfquet:
            df_final.write\
            .mode("overwrite")\
            .parquet(caminho_parquet)
            print(f"Dados do clustering KMeans gravados no formato Parquet em: {caminho_parquet}")
        print("Processo de clustering KMeans concluído com sucesso.")  