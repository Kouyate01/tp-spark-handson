import pytest
from pyspark.sql import SparkSession
from src.fr.hymaia.exo2.spark_clean_job import main  # Assurez-vous que le chemin est correct

@pytest.fixture(scope='module')
def spark():
    spark = SparkSession.builder \
        .appName("Integration Test") \
        .getOrCreate()
    yield spark
    spark.stop()

def test_integration_job(spark):
    # Exécutez le job principal et vérifiez le fichier de sortie
    main()  # Assurez-vous que main() exécute tout le processus
    output_df = spark.read.csv("data/exo2/clean", header=True, inferSchema=True)
    assert output_df.count() > 0  # Vérifiez que le fichier de sortie n'est pas vide

