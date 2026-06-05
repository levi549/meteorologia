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
            table="(SELECT MIN(id_referencia) as min_id, MAX(id_referencia) as max_id FROM public.dados_csv_silver) as limites",
            properties=PROPRIEDADES
        ).first()
            
        v_min = limites["min_id"] 
        v_max = limites["max_id"] 

        df=spark.read.jdbc( 
        url=URL_SUPABASE,
        table=f""" (
            SELECT * FROM public.dados_csv_silver
            WHERE id_referencia > (
                SELECT COALESCE(MAX(ultimo_id_processados), 0) 
                FROM public.log_job
                WHERE nome_job = 'kmeans_job' 
                AND status = 'SUCCESS'
            )
            AND id_referencia <= {v_max}
        ) as dados
        """,
        column="id_referencia",
        lowerBound=v_min,
        upperBound=v_max,
        numPartitions=10,
        properties=PROPRIEDADES
        ).dropna()
        janela=Window.partitionBy('city_id')
            
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
        kmeans=ML_kmeans()
        kmeans.treino(df_features)
        df_final=kmeans.predict(df_features).select(
            'id',
            "id_referencia",
            "anomaly_name",
            "mes_sin",
            "mes_cos",
            "temp",
            "humidity",
            "pressure",
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