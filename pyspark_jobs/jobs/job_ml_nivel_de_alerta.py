from pyspark.sql import functions as F
def job_ml_nivel_de_alerta(URL_SUPABASE, PROPRIEDADES, spark):
    try:
        print("Iniciando o processo de classificação supervisionada")
   
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
            SELECT * FROM public.kmeans_resultado 
            WHERE id_referencia > (
                SELECT COALESCE(MAX(ultimo_id_processados), 0) 
                FROM public.log_pipeline 
                WHERE nome_pipeline = 'kmeans_job' 
                AND status = 'SUCCESS'
            )
        ) as dados
        """,
        column="id_referencia",
        lowerBound=v_min,
        upperBound=v_max,
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
        ml_supervisionado=ML_supervisionado()
        ml_supervisionado.treino(df_features)
      
        df_final.write.jdbc(
            url=URL_SUPABASE,
            table="public.classificacao_supervisionada_resultado",
            mode="overwrite",
            properties=PROPRIEDADES
        )
    except Exception as e:
        print(f"Erro ao executar o processo de classificação supervisionada: {e}")
        raise e