import os
from datetime import date

import requests

HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/datalake/"
API_KEY = "642c58c7347f406237e7cbb0be02e21b"


def fetch_data_from_lastfm(**kwargs):
    tags_to_fetch = ["rock", "indie", "indie rock", "alternative", "pop"]

    for i in range(len(tags_to_fetch)) :
        tag = tags_to_fetch[i]
        fetch_artists(tag)
        fetch_tracks(tag)


def fetch_artists(tag):
    current_day = date.today().strftime("%Y%m%d")
    TARGET_PATH = DATALAKE_ROOT_FOLDER + "raw/lastfm/topArtist/" + tag + "/" + current_day + "/"
    if not os.path.exists(TARGET_PATH):
        os.makedirs(TARGET_PATH)

    url = 'https://ws.audioscrobbler.com/2.0/?method=tag.gettopartists&tag=' + tag + '&limit=1000' + '&api_key=' + API_KEY + '&format=json'
    r = requests.get(url)
    print("Writing here: ", TARGET_PATH)
    open(TARGET_PATH + 'lastfm.json', 'wb').write(r.content)

def fetch_tracks(tag):
    current_day = date.today().strftime("%Y%m%d")
    TARGET_PATH = DATALAKE_ROOT_FOLDER + "raw/lastfm/topTracks/" + tag + "/" + current_day + "/"
    if not os.path.exists(TARGET_PATH):
        os.makedirs(TARGET_PATH)

    url = 'https://ws.audioscrobbler.com/2.0/?method=tag.gettoptracks&tag=' + tag + '&limit=1000' + '&api_key=' + API_KEY + '&format=json'
    r = requests.get(url)
    print("Writing here: ", TARGET_PATH)
    open(TARGET_PATH + 'lastfm.json', 'wb').write(r.content)


