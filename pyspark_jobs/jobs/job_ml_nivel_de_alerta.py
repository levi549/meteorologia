from pyspark.sql import functions as F
from src.logs import log
from src.ML import ML_nivel_de_alerta
def job_ml_nivel_de_alerta(URL_SUPABASE, PROPRIEDADES, spark,read_parquet=None,id=None):
    gerenciador=log()
    with gerenciador.log_job("ml_nivel_de_alerta",id) as logger:
        print("Iniciando o processo de classificação supervisionada")
        if not read_parquet:
            limites = spark.read.jdbc(
                url=URL_SUPABASE,
                table=""""(SELECT (
                SELECT COALESCE(MAX(ultimo_id_processados), 0) 
                FROM public.log_job
                WHERE nome_job = 'kmeans_job' 
                AND status = 'SUCCESS'
            ) as min_id, MAX(id_referencia) as max_id FROM public.dados_csv_silver) as limites""",
                properties=PROPRIEDADES
            ).first()
        
            v_min = limites["min_id"] 
            v_max = limites["max_id"] 

            df=spark.read.jdbc(
            url=URL_SUPABASE,
            table=f""" (
            SELECT * FROM public.dados_csv_silver
            WHERE id_referencia > {v_min}
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
        else:
            caminho_atual = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
            caminho_parquet = os.path.join(caminho_atual, "parquet")
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
        ml_supervisionado=ML_supervisionado()
        ml_supervisionado.treino(df_features)
      
        df_final.write.jdbc(
            url=URL_SUPABASE,
            table="public.classificacao_supervisionada_resultado",
            mode="overwrite",
            properties=PROPRIEDADES
        )
