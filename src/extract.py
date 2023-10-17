import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials, SpotifyOAuth
from datetime import datetime

from dotenv import load_dotenv

import pandas as pd

load_dotenv()

client_id = os.getenv("CLIENT_ID")
client_secret = os.getenv("CLIENT_SECRET")

scope = "user-read-recently-played"

import spotipy
from spotipy.oauth2 import SpotifyOAuth

# Define the desired scopes as a list
scopes = ["user-read-recently-played", "user-library-read"]

# Initialize the SpotifyOAuth object with your client ID, client secret, and desired scopes
sp_oauth = SpotifyOAuth(
    client_id=client_id,
    client_secret=client_secret,
    redirect_uri="http://localhost:8888/callback",
    scope=" ".join(scopes),
)

# # # Generate the authorization URL
# auth_url = sp_oauth.get_authorize_url()

# # Redirect the user to the generated auth_url to start the authorization process
# print(f"Click the following link to authorize your application: {auth_url}")

sp = spotipy.Spotify(client_credentials_manager=sp_oauth)

# GET the recently played tracks
recently_played = sp.current_user_recently_played()

## Save the json to df

song_names = []
artist_names = []
played_at_list = []
timestamps = []

# Extracting only the relevant bits of data from the json object
for song in recently_played["items"]:
    song_names.append(song["track"]["name"])
    artist_names.append(song["track"]["album"]["artists"][0]["name"])
    played_at_list.append(song["played_at"])
    timestamps.append(song["played_at"][0:10])

# Prepare a dictionary in order to turn it into a pandas dataframe below
song_dict = {
    "song_name": song_names,
    "artist_name": artist_names,
    "played_at": played_at_list,
    "timestamp": timestamps,
}

song_df = pd.DataFrame(
    song_dict, columns=["song_name", "artist_name", "played_at", "timestamp"]
)
