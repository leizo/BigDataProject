
from datetime import date
import os
from pendulum import duration
from pyrsistent import s
from pyspark.sql import SparkSession
from sqlalchemy import true
import json


HOME = os.path.expanduser('~')
JSON_PATH = HOME + "/airflow/dags/lib/"

current_day = date.today().strftime("%Y%m%d")

spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

#create list to parse lastfm's parquets
tags_to_parse = ["rock", "indie", "alternative", "pop"]
list_lastfm_tracks = []

for e in tags_to_parse :
    df = spark.read.parquet(HOME + "/datalake/formatted/lastfm/" + "topTracks" + "/" + e + "/" + current_day + "/lastfm.parquet")
    list_lastfm_tracks.append(df)

list_lastfm_artists = []

for e in tags_to_parse :
    df = spark.read.parquet(HOME + "/datalake/formatted/lastfm/" + "topArtist" + "/" + e + "/" + current_day + "/lastfm.parquet")
    list_lastfm_artists.append(df)

#create list to parse spotify's parquets
file = open(JSON_PATH + "playlists_to_fetch.json")
playlists_to_fetch = json.load(file)
list_of_playlists = playlists_to_fetch["playlists"]

list_spotify_playlists = []

for e in list_of_playlists :
    df = spark.read.parquet(HOME + "/datalake/formatted/spotify/playlists/" + e + "/" + current_day + "/spotify.parquet")
    list_spotify_playlists.append(df)


def produce_usage() :

    for i in range(len(list_lastfm_tracks)) :
        df_joined_bytracks_fromtag(list_lastfm_tracks[i], tags_to_parse[i], current_day)
        df_joined_byartist_fromtag(list_lastfm_artists[i], tags_to_parse[i], current_day)
        

def df_joined_bytracks_fromtag(lastfm_tracks, tag, date) :
    DF_tag_playlists = lastfm_tracks.join(list_spotify_playlists[0],"name")

    list_spotify_playlists.pop(0)

    for e in list_spotify_playlists :
        df_join = lastfm_tracks.join(e,"name")
        DF_tag_playlists = DF_tag_playlists.union(df_join) \
    
    DF_tag_playlists.dropDuplicates() \
            .drop("duration_s") \
            .drop("artist_name")
    
    TARGET_PATH = HOME + "/datalake/usage/recommendation/bytracks/" + tag + "/" + date + "/usage.parquet"

    try :
        DF_tag_playlists.write.parquet(TARGET_PATH)
        print("Writing here: ", TARGET_PATH)
        
    except :
        print("paquet already exists /bytracks/" + tag + "/" + date)
 
def df_joined_byartist_fromtag(lastfm_tracks, tag, date) :
    DF_tag_playlists = lastfm_tracks.join(list_spotify_playlists[0],"artist")

    list_spotify_playlists.pop(0)

    for e in list_spotify_playlists :
        df_join = lastfm_tracks.join(e,"artist")
        DF_tag_playlists = DF_tag_playlists.union(df_join) \
    
    DF_tag_playlists.dropDuplicates() \
            .drop("artist_name")
    
    TARGET_PATH = HOME + "/datalake/usage/recommendation/byartist/" + tag + "/" + date + "/usage.parquet"

    try :
        DF_tag_playlists.write.parquet(TARGET_PATH)
        print("Writing here: ", TARGET_PATH)
        
    except :
        print("paquet already exists /byartist/" + tag + "/" + date)

def show_formatted(by, tag, date) :
    parDF=spark.read.parquet(HOME + "/datalake/usage/recommendation/" + by + "/" + tag + "/" + date + "/usage.parquet")
    parDF.printSchema()
    parDF.show()

#show_formatted("byartist", "rock")
produce_usage()