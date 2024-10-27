from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

def filter_adults(df):
    """Filtrer les clients majeurs (âge >= 18)."""
    return df.filter(col("age") >= 18)

def join_with_cities(clients_df, cities_df):
    """Joindre les DataFrames de clients et de villes sur le code postal."""
    return clients_df.join(cities_df, "zip", "inner")

def extract_department(zip_code):
    """Extraire le département à partir du code postal."""
    if int(zip_code) <= 20190:
        return zip_code[:2] + "A"  # Corse
    else:
        return zip_code[:2] + "B"  # Autres départements

def add_department_column(df):
    """Ajouter la colonne département à la DataFrame."""
    return df.withColumn("departement", expr("substring(zip, 1, 2)"))

def main():
    # Créer la SparkSession
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("clean_job") \
        .getOrCreate()

    # Lire les fichiers clients et villes
    clients_df = spark.read.csv("/home/movibes/spark-handson/src/resources/exo2/clients_bdd.csv", header=True, inferSchema=True)
    cities_df = spark.read.csv("/home/movibes/spark-handson/src/resources/exo2/city_zipcode.csv", header=True, inferSchema=True)

    '''clients_df = spark.read.csv("src/resources/exo2/clients_bdd.csv", header=True, inferSchema=True)
    cities_df = spark.read.csv("src/resources/exo2/city_zipcode.csv", header=True, inferSchema=True)'''

    # Filtrer les clients majeurs
    adults_df = filter_adults(clients_df)

    # Joindre avec les villes
    result_df = join_with_cities(adults_df, cities_df)

    # Ajouter la colonne département
    result_df = add_department_column(result_df)

    # Écrire le résultat au format parquet
    result_df.write.mode("overwrite").parquet("data/exo2/clean")

    # Arrêter la SparkSession
    spark.stop()

if __name__ == "__main__":
    main()
