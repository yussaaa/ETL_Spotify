import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials, SpotifyOAuth
from datetime import datetime
import pandas as pd
import logging

from dotenv import load_dotenv

from azure.storage.filedatalake import (
    DataLakeServiceClient,
    DataLakeDirectoryClient,
    FileSystemClient,
)
from azure.identity import DefaultAzureCredential


def function_app_extract_recent_played_using_spotipy():
    """Helper function to extract recently played tracks using library Spotipy

    Returns:
        json: API returned JSON object, details at https://developer.spotify.com/documentation/web-api/reference/get-recently-played
    """

    load_dotenv()
    logging.info("-------- Loading environment variables ---------")

    client_id = os.getenv("CLIENT_ID")
    client_secret = os.getenv("CLIENT_SECRET")
    refresh_token = os.getenv("CLIENT_REFRESH_TOKEN")
    REDIRECTED_URL = "https://function-test-yusa.azurewebsites.net/api/http_trigger"

    logging.info(
        f"-------- Evironment Variables client_id, client_secret, refresh_token ---> Loaded ! ----------"
    )

    # Initialize the SpotifyOAuth object with your client ID, client secret, and desired scopes
    sp_oauth = SpotifyOAuth(
        client_id,
        client_secret,
        REDIRECTED_URL,
    )

    # Use the refresh token to get a new access token
    new_token = sp_oauth.refresh_access_token(refresh_token)["access_token"]
    sp = spotipy.Spotify(auth=new_token)

    logging.info("--------Spotipy connection made!----------")

    # GET the recently played tracks
    recently_played = sp.current_user_recently_played()

    logging.info("---------Playlist retrieved !----------")

    return recently_played


def add_timestamp_to_file(file_name: str):
    """Add timestamp to the JSON file

    Args:
        file_name (str): The name of the JSON file

    Returns:
        str: Modified JSON file name
    """
    # Get the current date in YYYYMMDD format
    current_date = datetime.now().strftime("%Y%m%d")

    # Generate the unique identifier using hour, minute, and second
    unique_id = datetime.now().strftime("%H%M%S")

    # Construct the file name
    file_name_w_timestamp = f"{file_name}_{current_date}_{unique_id}.JSON"

    return file_name_w_timestamp


def save_to_data_lake(obj, directory_on_lake: str = "/dev/bronze/"):
    """Helper function to save data to azure data lake.
        Requires azure storage account name and secret.

    Args:
        obj (_type_): The object to save.
        directory_on_lake (str, optional): The directory to save on the data lake container. Defaults to "dev/bronze/".
    """
    # Retrieve the connection details
    account_name = os.environ["STORAGE_ACCOUNT_NAME"]
    account_key = os.environ["STORAGE_ACCOUNT_KEY"]  # If you're using account key

    service_client = DataLakeServiceClient(
        account_url=f"https://{account_name}.dfs.core.windows.net",
        credential=account_key,
    )
    file_system_client = service_client.get_file_system_client(
        "spotify-etl" + directory_on_lake
    )

    file_name = add_timestamp_to_file("recently_played")

    # Assuming you're saving to the root of the container for simplicity
    file_client = file_system_client.get_file_client(file_name)

    file_client.upload_data(obj, overwrite=True)
