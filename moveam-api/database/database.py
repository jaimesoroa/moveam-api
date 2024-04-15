import pandas as pd
import requests
import sqlalchemy as db
from sqlalchemy import MetaData, delete
from datetime import datetime
# import datetime as dt
import boto3
from botocore.exceptions import ClientError
import json

# AWS secrets Database
def get_secrets_database():

    secret_name = "db_tarragona_historico_Jaime"
    region_name = "eu-west-3"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    # Decrypts secret using the associated KMS key.
    secret = get_secret_value_response['SecretString']
    
    return json.loads(secret)

def database_connection(cups, username, password, host, database, month, df, year, db_table):
    url_object = db.URL.create(
        "mysql+mysqldb",
        username=username,
        password=password,
        host=host,
        database=database,
    )

    engine = db.create_engine(url_object, pool_pre_ping=True)
    
    try:
        # Initialize the Metadata Object
        META_DATA = MetaData()
        MetaData.reflect(META_DATA, bind=engine)
        ELECTRICIDAD = META_DATA.tables[db_table]
        
        # Use a single transaction for all deletions
        with engine.begin() as conn:
            # Check if there has already been an update today.
            sql = f'select * from ELECTRICIDAD where month = {month} and year = {year} and cups = "{cups}"'
            df = pd.read_sql(sql,con=engine)
            if len(df) == 0 or (df['lastUpdated'][0].strftime('%Y-%m-%d') != datetime.today().strftime('%Y-%m-%d')):
                # Delete the previously updated month (before today)
                dele = ELECTRICIDAD.delete().where(ELECTRICIDAD.c.month == int(month)).where(ELECTRICIDAD.c.cups == cups).where(ELECTRICIDAD.c.year == int(year))
                conn.execute(dele)
                print(f'Month {month} deleted for CUPS {cups} at {datetime.today().strftime("%Y-%m-%d %H:%M:%S.%f")}')
                
                # Insert whole DataFrame into MySQL
                df.drop_duplicates(subset=['cups', 'date', 'time', 'month', 'year'], keep='last', inplace=True, ignore_index=True)
                df.to_sql('ELECTRICIDAD', con=engine, if_exists='append', chunksize=2000, index=False)
                print(f'DataFrame from CUPS {cups} and month {month} written in database at {datetime.today().strftime("%Y-%m-%d %H:%M:%S.%f")}')
            else:
                print(f'CUPS {cups} has already been updated at {datetime.today().strftime("%Y-%m-%d")}')
    except Exception as e:
        print(f"An error occurred for CUPS {cups} when trying to delete and write in database: {e}")
    finally:
        engine.dispose()
    
    return None

def database_writing(username, password, host, database, month, df, year, db_table):
    url_object = db.URL.create(
        "mysql+mysqldb",
        username=username,
        password=password,
        host=host,
        database=database,
    )

    engine = db.create_engine(url_object, pool_pre_ping=True)
    
    try:
        # Use a single transaction for all deletions
        with engine.begin() as conn:
                # Insert whole DataFrame into MySQL
                df.to_sql(db_table, con=engine, if_exists='append', chunksize=2000, index=False)
                print(f'DataFrame from month {month} and year {year} written in database at {datetime.today().strftime("%Y-%m-%d %H:%M:%S.%f")}')
    except Exception as e:
        print(f"An error occurred for month {month} and year {year} when trying to write in database: {e}")
    finally:
        engine.dispose()
    
    return None