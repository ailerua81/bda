from pyspark.sql import SparkSession
import os

def test_spark_setup():
    print(" Test de configuration PySpark...\n")
    
    # Créer la session
    spark = SparkSession.builder \
        .appName("BDA-Test") \
        .config("spark.executor.memory", "2g") \
        .getOrCreate()
    
    print(f" Spark version: {spark.version}")
    
    # Test avec données de prix
    prices_path = os.path.expanduser("~/bda-project/data/prices/*.csv")
    
    if os.path.exists(os.path.dirname(prices_path.replace("*", ""))):
        print(f"\n Chargement des données depuis : {prices_path}")
        
        df = spark.read.option("header", True) \
            .option("inferSchema", True) \
            .csv(prices_path)
        
        print(f" Données chargées : {df.count():,} lignes")
        print(f" Colonnes : {df.columns}")
        
        df.show(5)
    else:
        print(f" Répertoire de données non trouvé")
    
    spark.stop()
    print("\n Test terminé avec succès!")

if __name__ == "__main__":
    test_spark_setup()
