from pyspark.sql import SparkSession
import functions as fu
import sys

if __name__ == "__main__":
    spark = (
        SparkSession
        .builder
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )
    fu.run_trusted(spark)