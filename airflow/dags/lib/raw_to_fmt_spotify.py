from datetime import date
from importlib.resources import path
import json
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, to_date, when

spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

HOME = os.path.expanduser('~')
JSON_PATH = HOME + "/airflow/dags/lib/"

PATH = "/datalake/raw/spotify/playlists/"

def raw_to_fmt_spotify(**kwargs):
    current_day = date.today().strftime("%Y%m%d")
    
    file = open(JSON_PATH + "playlists_to_fetch.json")
    playlists_to_fetch = json.load(file)
    list_of_playlists = playlists_to_fetch["playlists"]

    for i in range(len(list_of_playlists)):
        playlist = list_of_playlists[i]
        try :
            raw_to_fmt_json(playlist, current_day)
        except :
            print("doesn't exist " + playlist + " " + current_day)

def simplify_items_json(playlist_name, date) :

    file = open(HOME + PATH + playlist_name + "/" + date + "/spotify.json")
    data = json.load(file)
    items = data["tracks"]["items"]

    map_of_test = []

    for i in range(len(items)) :
        test = {}
        test["name"] = items[i]["track"]["name"]
        test["popularity"] = items[i]["track"]["popularity"]
        test["duration_ms"] = items[i]["track"]["duration_ms"]
        test["track_number"] = items[i]["track"]["track_number"]
        test["artist"] = items[i]["track"]["artists"][0]["name"]
        test["album_name"] = items[i]["track"]["album"]["name"]
        test["release_date"] = items[i]["track"]["album"]["release_date"]
        test["album_total_tracks"] = items[i]["track"]["album"]["total_tracks"]

        map_of_test.append(test)
        
    with open(HOME + PATH + playlist_name + "/" + date + "/spotify_simplified.json", 'w') as fp:
        json.dump(map_of_test, fp)

def raw_to_fmt_json(playlist_name, date) :

    simplify_items_json(playlist_name, date)

    df = spark.read.json(HOME + PATH + playlist_name + "/" + date + "/spotify_simplified.json")

    df_transformed = df.withColumn("release_date", to_date(col("release_date")))

    TARGET_PATH = HOME + "/datalake/formatted/spotify/playlists/" + playlist_name + "/" + date + "/spotify.parquet"

    try :
        df_transformed.write.parquet(TARGET_PATH)
        print("Writing here: ", TARGET_PATH)
        
    except :
        print("paquet already exists " + playlist_name + " " + date)

def show_formatted(playlist_name, date) :
    parDF=spark.read.parquet(HOME + "/datalake/formatted/spotify/playlists/" + playlist_name + "/" + date + "/spotify.parquet")
    parDF.printSchema()
    parDF.show()

show_formatted("dailyMix1", "20220614")
#raw_to_fmt_spotify()    