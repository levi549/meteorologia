import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.logs import log
from src.ML import ML_kmeans
from pyspark.sql import functions as F
from pyspark.ml.feature import  VectorAssembler
from pyspark.sql.window import Window

from src.predicate import Predicate
def job_kmeans_train(URL_SUPABASE, PROPRIEDADES, spark,id=None):
    gerenciador=log()
    with gerenciador.log_job("kmeans_job_train",id) as logger:
        print("Iniciando o processo de treinamento do modelo KMeans")
        caminho_atual = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
        caminho_modelo = os.path.join(caminho_atual, "modelos/kmeans")
        limites = spark.read.jdbc(
            url=URL_SUPABASE,
            table="""(
            SELECT
                MIN(ingested_at) as min_dt,
                MAX(ingested_at) as max_dt,
                MIN(dt) as min_dt2,
                MAX(dt) as max_dt2
                from public.gold_dados_kmeans
                ) as limites
                """,
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
        ).dropna()
            
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
        logger.ultima_dt_processada=v_max