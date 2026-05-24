from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
import os
from pyspark.ml.feature import StringIndexer, OneHotEncoder

spark = SparkSession.builder \
    .appName("KMeans Clustering") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.2") \
    .getOrCreate()

URL_SUPABASE =os.getenv("SUPABASE_POSTGERESQL_URL")
PROPRIEDADES = {
    "user": "postgres",
    "password": os.getenv("SUPABASE_PASSWORD"),
    "driver": "org.postgresql.Driver"}


df=spark.read.jdbc(
    url=URL_SUPABASE,
    table="public.dados_csv_silver",
    properties=PROPRIEDADES
)
