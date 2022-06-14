from importlib.resources import path
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, to_date

HOME = os.path.expanduser('~')
PATH = "/datalake/raw/lastfm/topTracks/indie/20220614/lastfm.json"


spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
        
df = spark.read.option("multiline","true").json(HOME + PATH)
df.printSchema()



#df.show(vertical=True, truncate=False, n=1)



#df = df.withColumn("album_release_date_transformed",to_date("album_release_date")).show()


#df = spark.read.option("multiline","true").json(HOME + PATH)
#df.printSchema()
#df.show(vertical=True, truncate=False, n=1)

#df.select(col("tracks")).show(vertical=True, truncate=False)



#df.write.parquet(HOME + "/datalake/formatted/spotify.parquet")


#parDF=spark.read.parquet(HOME + "/datalake/formatted/spotify.parquet")
#parDF.show(truncate=False)
