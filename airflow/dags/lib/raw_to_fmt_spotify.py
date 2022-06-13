import os

import pandas as pd

HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/datalake/"


def convert_raw_to_formatted(file_name="spotify.json", current_day="20220607"):
   RATING_PATH = DATALAKE_ROOT_FOLDER + "raw/spotify/playlists/dailyMix1/" + current_day + "/" + file_name
   print("ok")
   FORMATTED_RATING_FOLDER = DATALAKE_ROOT_FOLDER + "formatted/spotify/playlists/dailyMix1/" + current_day + "/"
   if not os.path.exists(FORMATTED_RATING_FOLDER):
       os.makedirs(FORMATTED_RATING_FOLDER)
   df = pd.read_json(RATING_PATH)
   parquet_file_name = file_name.replace(".json", ".snappy.parquet")
   final_df = pd.DataFrame(data=df.data)
   final_df.to_parquet(FORMATTED_RATING_FOLDER + parquet_file_name)
