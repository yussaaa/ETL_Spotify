import azure.functions as func
import logging
import json


logging.info("----------- Dependent Packages Loaded -------------")

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


@app.route(route="http_trigger")
def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Importing the packages!")
    from src.packages.function_App_spotify_ETL import (
        function_app_extract_recent_played_using_spotipy,
        save_to_data_lake,
    )

    try:
        logging.info("---------Package loaded! Now calling spotify API! ----------")
        recently_played_json = json.dumps(
            function_app_extract_recent_played_using_spotipy()
        )
        logging.info("---------Playlist retrieved!----------")
        save_to_data_lake(recently_played_json)
        logging.info("---------Playlist Saved to datalake !----------")
        return func.HttpResponse(
            # json.dumps(recently_played_json), mimetype="application/json"
            "Successfully extracted Recently Played tracks history and saved to data lake"
        )
    except Exception as e:
        logging.error(f"Exception: {e}")
        return func.HttpResponse(
            f"Error running the extract spotify list function: {e}"
        )


@app.route(route="http_trigger_2")
def http_trigger_2(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Python HTTP trigger function processed a request.")

    name = req.params.get("name")

    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get("name")

    if name:
        return func.HttpResponse(
            f"Hello, {name}. This HTTP triggered function executed successfully is what you recently listened."
        )
    else:
        return func.HttpResponse(
            "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
            status_code=200,
        )
