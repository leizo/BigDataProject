
from copyreg import add_extension
from datetime import date
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json
import json

HOME = os.path.expanduser('~')
JSON_PATH = HOME + "/airflow/dags/lib/"

current_day = date.today().strftime("%Y%m%d")

tags_to_parse = ["rock", "indie", "alternative", "pop"]

def load_dataframes() :
    spark = SparkSession \
            .builder \
            .appName("Python Spark SQL basic example") \
            .config("spark.some.config.option", "some-value") \
            .getOrCreate()
            
    #create list to parse lastfm's parquets
    
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

    return list_lastfm_tracks, list_lastfm_artists, list_spotify_playlists

def produce_usage() :
    spark = SparkSession \
            .builder \
            .appName("Python Spark SQL basic example") \
            .config("spark.some.config.option", "some-value") \
            .getOrCreate()

    list_lastfm_tracks, list_lastfm_artists, list_spotify_playlists = load_dataframes()

    for i in range(len(list_lastfm_tracks)) :
        df_joined_bytracks_fromtag(list_lastfm_tracks[i], tags_to_parse[i], current_day, list_spotify_playlists)
        df_joined_byartist_fromtag(list_lastfm_artists[i], tags_to_parse[i], current_day, list_spotify_playlists)    

def df_joined_bytracks_fromtag(lastfm_tracks, tag, date, list_spotify_playlists) :
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
 
def df_joined_byartist_fromtag(lastfm_tracks, tag, date, list_spotify_playlists) :
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
    spark = SparkSession \
            .builder \
            .appName("Python Spark SQL basic example") \
            .config("spark.some.config.option", "some-value") \
            .getOrCreate()

    parDF=spark.read.parquet(HOME + "/datalake/usage/recommendation/" + by + "/" + tag + "/" + date + "/usage.parquet")
    parDF.printSchema()
    parDF.show()

def transform_to_json_for_elastic(by, tag, date) :
    spark = SparkSession \
            .builder \
            .appName("Python Spark SQL basic example") \
            .config("spark.some.config.option", "some-value") \
            .getOrCreate()

    parDF=spark.read.parquet(HOME + "/datalake/usage/recommendation/" + by + "/" + tag + "/" + date + "/usage.parquet")
    parDF.show(vertical=True)
    try : 
        parDF.coalesce(1).write.format('json').save(HOME + "/datalake/usage/recommendation/" + by + "/" + tag + "/" + date + "/usage.json")
    except :
        print("already exist")

"""

def transform_to_json_for_elastic(by, tag, date) :
    spark = SparkSession \
            .builder \
            .appName("Python Spark SQL basic example") \
            .config("spark.some.config.option", "some-value") \
            .getOrCreate()

    parDF=spark.read.parquet(HOME + "/datalake/usage/recommendation/" + by + "/" + tag + "/" + date + "/usage.parquet")
    parDF.show(vertical=True)
    #try : 
    #    parDF.coalesce(1).write.format('json').save(HOME + "/datalake/usage/recommendation/" + by + "/" + tag + "/" + date + "/usage.json")
    #except :
    #    print("already exist")

    res = requests.get('http://localhost:9200')
    print (res.content)
    es = Elasticsearch([{'host': 'localhost', 'port': '9200'}])
    es.index(index='myindex', ignore=400, doc_type='json', 
        id=i, body=json.loads(parDF.toJSON().first()))
    #parDFjson = parDF.to_json()
    #print(parDFjson)
   # open(HOME + "/datalake/usage/recommendation/" + by + "/" + tag + "/" + date + "/usage.json", 'wb').write(parDFjson)
"""

#show_formatted("byartist", "rock", current_day)
#produce_usage()

#transform_to_json_for_elastic("byartist", "alternative", current_day)
#transform_to_json_for_elastic("byartist", "pop", current_day)