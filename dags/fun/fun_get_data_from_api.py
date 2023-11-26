import requests
import pandas as pd

from airflow.models import Connection
from sqlalchemy import create_engine

def fun_get_data_from_api(**kwargs):
    api_url = "http://103.150.197.96:5005/api/v1/rekapitulasi_v2/jabar/harian"

    # Fetch data from the API
    response = requests.get(api_url)
    data = response.json()['data']['content']

    df = pd.DataFrame(data)

    print(df)

    config = Connection.get_connection_from_secrets("connect_mysql")
    db_params = {
        "user": config.login,
        "password": config.password,
        "host": config.host,
        "port": config.port,
        "database": config.schema
    }
    engine = create_engine(
        f'mysql+mysqlconnector://{db_params["user"]}:{db_params["password"]}@{db_params["host"]}:{db_params["port"]}/{db_params["database"]}')
    table_name = "covid_jabar"
    df.to_sql(table_name, engine, if_exists='replace', index=False)
    print("====================== Success to load data into MySQL ======================")
    engine.dispose()
