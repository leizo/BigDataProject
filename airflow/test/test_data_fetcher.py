from dags.lib.spotify_fetcher import fetch_data_from_spotify
from dags.lib.lastfm_fetcher import fetch_data_from_lastfm
from dags.lib.raw_to_fmt_spotify import raw_to_fmt_spotify

#fetch_data_from_spotify()
#fetch_data_from_lastfm()

raw_to_fmt_spotify()
