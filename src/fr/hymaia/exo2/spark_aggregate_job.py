from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

def calculate_population_by_department(df):
    """Calculer la population par département."""
    return df.groupBy("departement").agg(count("name").alias("nb_people")) \
             .orderBy(col("nb_people").desc(), col("departement"))

def main():
    # Créer la SparkSession
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("aggregate_job") \
        .getOrCreate()

    # Lire le fichier clean
    clean_df = spark.read.parquet("data/exo2/clean")

    # Calculer la population par département
    population_df = calculate_population_by_department(clean_df)

    # Écrire le résultat en CSV
    population_df.coalesce(1).write.mode("overwrite").csv("data/exo2/aggregate", header=True)

    # Arrêter la SparkSession
    spark.stop()

if __name__ == "__main__":
    main()
