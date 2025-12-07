from pyspark.sql import SparkSession
import os

spark = SparkSession.builder \
    .appName("BDA-Price-Check") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

prices_path = os.path.expanduser("~/bda-project/data/prices/*.csv")
print(f"Chemin : {prices_path}")

df = spark.read.option("header", True) \
    .option("inferSchema", True) \
    .csv(prices_path)

df.printSchema()
df.show(10, truncate=False)
print(f"Nombre de lignes : {df.count()}")

spark.stop()
