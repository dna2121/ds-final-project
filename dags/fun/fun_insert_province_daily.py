import pandas as pd

from airflow.models import Connection
from sqlalchemy import create_engine

def fun_insert_province_daily():
    config_mysql = Connection.get_connection_from_secrets("connect_mysql")
    config_pg = Connection.get_connection_from_secrets("connect_postgres")

    mysql_params = {
        "user": config_mysql.login,
        "password": config_mysql.password,
        "host": config_mysql.host,
        "port": config_mysql.port,
        "database": config_mysql.schema
    }
    pg_params = {
        "user": config_pg.login,
        "password": config_pg.password,
        "host": config_pg.host,
        "port": config_pg.port,
        "database": config_pg.schema
    }

    mysql_engine = create_engine(
        f'mysql+mysqlconnector://{mysql_params["user"]}:{mysql_params["password"]}@{mysql_params["host"]}:{mysql_params["port"]}/{mysql_params["database"]}')
    
    pg_engine = create_engine(
        f'postgresql://{pg_params["user"]}:{pg_params["password"]}@{pg_params["host"]}:{pg_params["port"]}/{pg_params["database"]}')


    # get 'province_id', 'date', 'total'
    sql = """SELECT * FROM covid_jabar"""
    df = pd.read_sql(sql, con=mysql_engine)
    df.info()

    # get 'case_id'
    sql_case = """SELECT id as case_id, status FROM dwh.public.dim_case"""
    df_dim_case = pd.read_sql(sql_case, con=pg_engine)

    # get the value (total)
    df_melt = pd.melt(df, id_vars=['kode_prov', 'tanggal'],
                    value_vars=['closecontact_dikarantina', 'closecontact_discarded', 'closecontact_meninggal', 
                               'confirmation_meninggal', 'confirmation_sembuh', 'probable_diisolasi', 'probable_discarded', 
                               'probable_meninggal', 'suspect_diisolasi', 'suspect_discarded', 'suspect_meninggal'],
                    value_name='total',
                    var_name='status'
                    )

    df_province_daily = pd.merge(df_melt, df_dim_case, on='status')
    df_province_daily.rename(columns={'kode_prov': 'province_id', 'tanggal': 'date'}, inplace=True) # modify column names
    df_province_daily['id'] = range(1, len(df_province_daily) + 1) #tambah kolom id
    df_province_daily = df_province_daily[['province_id', 'date', 'total', 'case_id', 'id']]

    # load province to postgresql
    table_name = "province_daily"
    df_province_daily.to_sql(table_name, pg_engine, if_exists='replace', index=False)
    print("============ Success to load province data into PostgreSQL ============")
    pg_engine.dispose()

