from datetime import datetime
# $WIPE_BEGIN
import pytz # pytz brings the Olson tz database into Python. This library allows accurate and cross platform timezone calculations
import pandas as pd

from moveam-api.database.database import get_secrets_database
from moveam-api.database.database import database_connection


from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

# http://127.0.0.1:8000/predict?pickup_datetime=2012-10-06 12:10:20&pickup_longitude=40.7614327&pickup_latitude=-73.9798156&dropoff_longitude=40.6513111&dropoff_latitude=-73.8803331&passenger_count=2
@app.get("/energy")
def predict(start_datetime: datetime,  # 2013-07-06 17:18:00
            end_datetime: datetime,  # 2013-07-06 17:18:00
            lapse: str, # hourly Puede ser 'monthly', 'weekly', 'daily' o 'hourly' (Este ultimo sera por defecto, no necesario añadirlo.
            tenant_id: int):      # 1
    """
    we use type hinting to indicate the data types expected
    for the parameters of the function
    FastAPI uses this information in order to hand errors
    to the developpers providing incompatible parameters
    FastAPI also provides variables of the expected data type to use
    without type hinting we need to manually convert
    the parameters of the functions which are all received as strings
    """

    # ⚠️ if the timezone conversion was not handled here the user would be assumed to provide an UTC datetime
    # create datetime object from user provided date
    # pickup_datetime = datetime.strptime(pickup_datetime, "%Y-%m-%d %H:%M:%S")

    # localize the user provided datetime with the NYC timezone
    madrid = pytz.timezone("Europe/Madrid")
    localized_pickup_datetime = madrid.localize(start_datetime, is_dst=None)

    # convert the user datetime to UTC and format the datetime as expected by the pipeline
    utc_pickup_datetime = localized_pickup_datetime.astimezone(pytz.utc)
    formatted_pickup_datetime = utc_pickup_datetime.strftime("%Y-%m-%d %H:%M:%S UTC")


    # ⚠️ fastapi only accepts simple python data types as a return value
    # among which dict, list, str, int, float, bool
    # in order to be able to convert the api response to json
    return dict(fare=float(y_pred))


@app.get("/")
def root():
    return dict(greeting="Hello")