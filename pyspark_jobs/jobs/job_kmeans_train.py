from src.logs import log
from src.ML import ML_kmeans
from pyspark.sql import functions as F
from pyspark.ml.feature import  VectorAssembler
from pyspark.sql.window import Window
import os
def job_kmeans_train(URL_SUPABASE, PROPRIEDADES, spark,id=None):
    gerenciador=log()
    with gerenciador.log_job("kmeans_job_train",id) as logger:
        print("Iniciando o processo de treinamento do modelo KMeans")
        caminho_atual = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
        caminho_parquet = os.path.join(caminho_atual, "modelos")
        limites = spark.read.jdbc(
            url=URL_SUPABASE,
            table="""(SELECT MIN(ingested_at) as min_dt, MAX(ingested_at) as max_dt FROM public.dados_gold_kmeans) as limites""",
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
        kmeans.save_model(caminho_modelo)