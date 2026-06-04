from src.ML import ML_kmeans
from pyspark.sql import functions as F
from pyspark.ml.feature import  VectorAssembler
from pyspark.sql.window import Window
def job_kmeans(URL_SUPABASE, PROPRIEDADES, spark):
    try:
        print("Iniciando o processo de clustering KMeans")
        
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
                FROM public.log_pipeline 
                WHERE nome_pipeline = 'dbt_silver_job' 
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
        print("Processo de clustering KMeans concluído com sucesso.")

    except Exception as e:
        print(f"Erro ao executar o processo de clustering KMeans: {e}")
        raise e