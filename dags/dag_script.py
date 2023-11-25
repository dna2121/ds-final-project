from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from datetime import datetime

import requests
import pandas as pd
import os
from sqlalchemy import create_engine
from airflow.models import Connection

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
    schema_name = "mysql"
    table_name = "covid_jabar"
    df.to_sql(table_name, engine, schema=schema_name, if_exists='replace', index=False)
    print("====================== Success to load data into MySQL ======================")
    engine.dispose()

#create dag
with DAG(
    dag_id='dag_script',
    schedule_interval='0 0 * * *',
    start_date=datetime(2023, 11, 23),
    catchup=True
) as dag :
    
    #task
    op_get_data_from_api = PythonOperator(
        task_id='get_data_from_api',
        python_callable=fun_get_data_from_api
    )

    op_generate_dim = EmptyOperator(
        task_id='generate_dim'
    )

    op_insert_district_daily = EmptyOperator(
        task_id='insert_district_daily'
    )

    op_insert_province_daily = EmptyOperator(
        task_id='insert_province_daily'
    )

    #flow in dag
    op_get_data_from_api >> op_generate_dim >> [op_insert_district_daily, op_insert_province_daily]

