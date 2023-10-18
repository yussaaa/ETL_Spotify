import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials, SpotifyOAuth
from datetime import datetime
import pandas as pd

import sqlite3
from dotenv import load_dotenv


def extract_recent_played_using_spotipy():
    """Helper function to extract recently played tracks using library Spotipy

    Returns:
        json: API returned JSON object, details at https://developer.spotify.com/documentation/web-api/reference/get-recently-played
    """

    load_dotenv()
    client_id = os.getenv("CLIENT_ID")
    client_secret = os.getenv("CLIENT_SECRET")

    # Define the desired scopes as a list
    scopes = ["user-read-recently-played", "user-library-read"]

    # Initialize the SpotifyOAuth object with your client ID, client secret, and desired scopes
    sp_oauth = SpotifyOAuth(
        client_id=client_id,
        client_secret=client_secret,
        redirect_uri="http://localhost:8888/callback",
        scope=" ".join(scopes),
    )

    sp = spotipy.Spotify(client_credentials_manager=sp_oauth)

    # GET the recently played tracks
    recently_played = sp.current_user_recently_played()

    return recently_played


def json_to_df(json_obj=None, **kwargs) -> pd.DataFrame:
    """This is a helper function to convert recently played songs to a padas dataframe

    Args:
        json_obj (json): Input JSON object
    """

    if not json_obj:
        json_obj = kwargs["ti"].xcom_pull(task_ids="task_1_extract")

    ## Save the json to df
    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []

    # Extracting only the relevant bits of data from the json object
    for song in json_obj["items"]:
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

    return song_df


def load_to_sqlite(df=None, **kwargs):
    if not df:
        df = kwargs["ti"].xcom_pull(task_ids="task_2_transform")

    conn = sqlite3.connect("spotify.db")

    # # Get the path to the database file
    # cursor.execute("PRAGMA database_list;")
    # database_path = cursor.fetchone()[2]

    # print("Database path:", database_path)

    # Use the to_sql method to write the DataFrame to a new table in the database
    table_name = "recently_played"  # Name for the new table in the database
    df.to_sql(table_name, conn, if_exists="replace", index=False)

    # Close the database connection
    conn.close()
