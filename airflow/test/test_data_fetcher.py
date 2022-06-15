from dags.lib.spotify_fetcher import fetch_data_from_spotify
from dags.lib.lastfm_fetcher import fetch_data_from_lastfm
from dags.lib.raw_to_fmt_spotify import raw_to_fmt_spotify
from dags.lib.raw_to_fmt_lastfm import raw_to_fmt_lastfm
from dags.lib.produce_usage import produce_usage

fetch_data_from_spotify()
fetch_data_from_lastfm()

raw_to_fmt_spotify()
raw_to_fmt_lastfm()

produce_usage()
