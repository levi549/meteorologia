from src.logs import log
from src.ML import ML_nivel_de_alerta
from pyspark.sql import functions as F
from pyspark.ml.feature import  VectorAssembler
from pyspark.sql.window import Window
import os
def job_ml_nivel_de_alerta_train(URL_SUPABASE, PROPRIEDADES, spark,read_parquet=None,id=None):
    gerenciador=log()
    with gerenciador.log_job("ml_nivel_de_alerta_train",id) as logger:
        print("Iniciando o processo de treinamento do modelo de classificação supervisionada")
        caminho_atual = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
        caminho_modelo = os.path.join(caminho_atual, "modelos", "ml_nivel_de_alerta_model")
        if not read_parquet:
            caminho_parquet = os.path.join(caminho_atual, "parquet")
            limites = spark.read.jdbc(
                url=URL_SUPABASE,
                table="""(SELECT MIN(ingested_at) as min_id, MAX(ingested_at) as max_id FROM public.kmeans_resultado) as limites""",
                properties=PROPRIEDADES
            ).first()

            v_min = limites["min_id"] 
            v_max = limites["max_id"] 

            df=spark.read.jdbc(
            url=URL_SUPABASE,
            table=f""" (
                SELECT * FROM public.kmeans_resultado
                WHERE ingested_at >= '{v_min}'
                AND ingested_at <= '{v_max}'
            ) as dados
            """,
            column="ingested_at",
            lowerBound=str(v_min),
            upperBound=str(v_max),
            numPartitions=10,
            properties=PROPRIEDADES
            ).dropna()
                
            assembler=VectorAssembler(inputCols=[
                "mes_sin", 
                "mes_cos",
                "temp",
                "humidity",
                "pressure",
                "Nivel_de_alerta"
            ], outputCol="features")
            df_features=assembler.transform(df)
            ml_supervisionado=ML_nivel_de_alerta()
            ml_supervisionado.treino(df_features)
            ml_supervisionado.save_model(caminho_modelo)
            logger.ultimo_id_processado = v_max
        else:
            df=spark.read.parquet(caminho_parquet).dropna()
            assembler=VectorAssembler(inputCols=[
                "mes_sin", 
                "mes_cos",
                "temp",
                "humidity",
                "pressure",
                "Nivel_de_alerta"
                ], outputCol="features")
            df_features=assembler.transform(df)
            ml_supervisionado=ML_nivel_de_alerta()
            ml_supervisionado.treino(df_features)
            ml_supervisionado.save_model(caminho_modelo)
            logger.ultimo_id_processado = df.agg({"ingested_at": "max"}).collect()[0][0]
    print("Treinamento do modelo de classificação supervisionada concluído")

