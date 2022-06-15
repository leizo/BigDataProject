from datetime import date
from importlib.resources import path
import json
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, to_date, when
from pyspark.sql.types import IntegerType, LongType

spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

HOME = os.path.expanduser('~')
JSON_PATH = HOME + "/airflow/dags/lib/"

PATH = "/datalake/raw/lastfm/"

def raw_to_fmt_lastfm(**kwargs):
    tags_to_parse = ["rock", "indie", "indie rock", "alternative", "pop"]
    current_day = date.today().strftime("%Y%m%d")
    
    for e in ["topArtist", "topTracks"] :
        for i in range(len(tags_to_parse)):
            tag = tags_to_parse[i]
            try :
                raw_to_fmt_json(e, tag, current_day)
            except :
                print("doesn't exist " + e + " " + tag + " " + current_day)

def simplify_items_json(top, tag, date) :

    file = open(HOME + PATH + top + "/" + tag + "/" + date + "/lastfm.json")
    data = json.load(file)

    if top == "topArtist" : 
        items = data["topartists"]["artist"]

        map_of_test = []

        for i in range(len(items)) :
            test = {}
            test["rank_artist_"+tag] = items[i]["@attr"]["rank"]
            test["artist"] = items[i]["name"]

            map_of_test.append(test)


    elif top == "topTracks" :
        
        items = data["tracks"]["track"]

        map_of_test = []

        for i in range(len(items)) :
            test = {}
            test["name"] = items[i]["name"]
            test["duration_s"] = items[i]["duration"]
            test["rank_tag_"+tag] = items[i]["@attr"]["rank"]
            test["artist_name"] = items[i]["artist"]["name"]

            map_of_test.append(test)
        
    with open(HOME + PATH + top + "/" + tag + "/" + date + "/lastfm_simplified.json", 'w') as fp:
        json.dump(map_of_test, fp)

def raw_to_fmt_json(top, tag, date) :

    simplify_items_json(top, tag, date)

    df = spark.read.json(HOME + PATH + top + "/" + tag + "/" + date + "/lastfm_simplified.json")

    if top == "topArtist" : 
        df_transformed = df.withColumn("rank_artist_"+tag, df["rank_artist_"+tag].cast(LongType()))

    elif top == "topTracks" :
        df_transformed = df.withColumn("duration_s", df["duration_s"].cast(LongType())) \
                            .withColumn("rank_tag_"+tag, df["rank_tag_"+tag].cast(LongType()))

    TARGET_PATH = HOME + "/datalake/formatted/lastfm/" + top + "/" + tag + "/" + date + "/lastfm.parquet"

    try :
        df_transformed.write.parquet(TARGET_PATH)
        print("Writing here: ", TARGET_PATH)
        
    except :
        print("paquet already exists " + top + " " + tag + " " + date)

    

def show_formatted(top, tag, date) :
    parDF=spark.read.parquet(HOME + "/datalake/formatted/lastfm/" + top + "/" + tag + "/" + date + "/lastfm.parquet")
    parDF.printSchema()
    parDF.show()

    
#show_formatted("topTracks", "indie", "20220614")
raw_to_fmt_lastfm()