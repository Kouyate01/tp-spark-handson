from pyspark.sql import SparkSession

def main():
    # Création de la SparkSession
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("wordcount") \
        .getOrCreate()


    # Lire le fichier
    df = spark.read.csv("src/resources/exo1/data.csv", header=True)

    # Appliquer la fonction wordcount
    wordcount = df.selectExpr("explode(split(text, ' ')) as word") \
        .groupBy("word") \
        .count()

    # Écrire le résultat dans le format parquet, en écrasant le répertoire existant
    wordcount.write.mode("overwrite").partitionBy("count").parquet("data/exo1/output")

    # Arrêter la SparkSession
    spark.stop()

if __name__ == "__main__":
    main()






'''import pyspark.sql.functions as f
from pyspark.sql import SparkSession


def main():
    print("Hello world!")
'''

def wordcount(df, col_name):
    return df.withColumn('word', f.explode(f.split(f.col(col_name), ' '))) \
        .groupBy('word') \
        .count()