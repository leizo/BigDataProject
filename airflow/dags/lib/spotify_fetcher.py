import os
from datetime import date
import json

import requests

HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/datalake/"
JSON_PATH = HOME + "/airflow/dags/lib/"

def fetch_data_from_spotify(**kwargs):
    file = open(JSON_PATH + "playlists_to_fetch.json")
    playlists_to_fetch = json.load(file)
    list_of_playlists = playlists_to_fetch["playlists"]

    for i in range(len(list_of_playlists)):
        playlist = list_of_playlists[i]
        fetch_data_from_playlist(playlist, playlists_to_fetch[playlist])


def fetch_data_from_playlist(playlist, id):
    current_day = date.today().strftime("%Y%m%d")
    TARGET_PATH = DATALAKE_ROOT_FOLDER + "raw/spotify/playlists/"+ playlist + "/" + current_day + "/"
    if not os.path.exists(TARGET_PATH):
        os.makedirs(TARGET_PATH)

    url = 'https://api.spotify.com/v1/'
    r = requests.get(url + 'playlists/' + id, headers=auth_to_spotify())
    print("Writing here: ", TARGET_PATH)
    open(TARGET_PATH + 'spotify.json', 'wb').write(r.content)

def auth_to_spotify():
    CLIENT_ID = '14a46a1dabb0473fa5ba4c06ba3508e6'
    CLIENT_SECRET = '7338f96a72984edcbdc96710f218f8e2'

    AUTH_URL = 'https://accounts.spotify.com/api/token'

    # POST
    auth_response = requests.post(AUTH_URL, {
        'grant_type': 'client_credentials',
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
    })

    # convert the response to JSON
    auth_response_data = auth_response.json()
    # save the access token
    access_token = auth_response_data['access_token']

    headers = {
        'Authorization': 'Bearer {token}'.format(token=access_token)
    }

    return headers