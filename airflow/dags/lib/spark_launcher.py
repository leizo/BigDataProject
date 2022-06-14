
from pyspark.sql import SparkSession

def spark_launcher() :
    spark = SparkSession \
            .builder \
            .appName("Python Spark SQL basic example") \
            .config("spark.some.config.option", "some-value") \
            .getOrCreate()
    return spark