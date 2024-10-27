import pytest
from pyspark.sql import SparkSession
from src.fr.hymaia.exo2.spark_clean_job import filter_adults, join_with_cities
'''from fr.hymaia.exo2.spark_clean_job import filter_adults  # Assurez-vous que le chemin est correct
from fr.hymaia.exo2.spark_clean_job import join_clients_and_cities
'''

@pytest.fixture(scope='module')
def spark():
    spark = SparkSession.builder \
        .appName("Test App") \
        .getOrCreate()
    yield spark
    spark.stop()

def test_filter_adults(spark):
    data = [("Alice", 25, "75001"), ("Bob", 17, "75002")]
    columns = ["name", "age", "zip"]
    df = spark.createDataFrame(data, columns)
    result_df = filter_adults(df)
    expected_data = [("Alice", 25, "75001")]
    expected_df = spark.createDataFrame(expected_data, columns)
    assert result_df.collect() == expected_df.collect()

def test_filter_adults_error(spark):
    # Tester un cas où le DataFrame est vide
    data = []
    columns = ["name", "age", "zip"]
    df = spark.createDataFrame(data, columns)
    result_df = filter_adults(df)
    assert result_df.count() == 0

def join_with_cities(spark):
    clients_data = [("Alice", 25, "75001"), ("Bob", 30, "75002")]
    cities_data = [("75001", "Paris"), ("75002", "Lyon")]
    clients_df = spark.createDataFrame(clients_data, ["name", "age", "zip"])
    cities_df = spark.createDataFrame(cities_data, ["zip", "city"])
    result_df = join_with_cities(clients_df, cities_df)
    expected_data = [("Alice", 25, "75001", "Paris"), ("Bob", 30, "75002", "Lyon")]
    expected_df = spark.createDataFrame(expected_data, ["name", "age", "zip", "city"])
    assert result_df.collect() == expected_df.collect()
