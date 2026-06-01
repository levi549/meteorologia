from pyspark.sql import functions as F
def job_ml_supervisionado(URL_SUPABASE, PROPRIEDADES, spark):
    try:
        print("Iniciando o processo de classificação supervisionada")
   
        limite=spark.read.jdbc(
            url=URL_SUPABASE,
            table="(SELECT MIN(id) as min_id, MAX(id) as max_id FROM public.kmeans_resultado) as limite",
            properties=PROPRIEDADES
        ).first()
        v_min = limite["min_id"]
        v_max = limite["max_id"]
    except Exception as e:
        print(f"Erro ao executar o processo de classificação supervisionada: {e}")
        raise e