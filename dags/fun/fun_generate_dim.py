import pandas as pd

from airflow.models import Connection
from sqlalchemy import create_engine

def fun_generate_dim(**kwargs):
    # connect to staging area (mySql)
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


    # fetch data from staging area
    sql = """SELECT * FROM covid_jabar"""
    df = pd.read_sql(sql, con=mysql_engine)
    df.info()

    ## PROVINCE
    # transform province data
    selected_province_column = ['kode_prov', 'nama_prov'] # select the 'province' columns
    df_province = df[selected_province_column]
    df_province.rename(columns={'kode_prov': 'province_id', 'nama_prov': 'province_name'}, inplace=True) # modify column names
    df_province.drop_duplicates(inplace=True) # delete the duplicates data
    print(df_province)

    # load province to postgresql
    table_name = "dim_province"
    df_province.to_sql(table_name, pg_engine, if_exists='replace', index=False)
    print("============ Success to load province data into PosgreSQL ============")
    pg_engine.dispose()

    ## DISTRICT
    # transform district data
    selected_district_column = ['kode_kab', 'kode_prov', 'nama_kab'] # select the 'district' columns
    df_district = df[selected_district_column]
    df_district.rename(columns={'kode_kab': 'district_id', 'kode_prov': 'province_id', 'nama_kab': 'district_name'}, inplace=True) # modify column names
    df_district.drop_duplicates(inplace=True) # delete the duplicates data
    print(df_district)

    # load district to postgresql
    table_name = "dim_district"
    df_district.to_sql(table_name, pg_engine, if_exists='replace', index=False)
    print("============ Success to load district data into PostgreSQL ============")
    pg_engine.dispose()

    ## CASE
    # transform case data
    print("check nama kolom : ")
    print(df.columns)

    filtered_columns = [column for column in df.columns if not column.startswith(('ko', 'na')) and '_' in column]
    df_status = pd.DataFrame({'status': filtered_columns}).drop_duplicates() #buat kolom 'status' di DataFrame status
    df_status[['status_name', 'status_detail']] = df_status['status'].str.split('_', expand=True) #tambah kolom di DataFrame
    df_status['id'] = range(1, len(df_status) + 1) #tambah kolom id di DataFrame status
    print(df_status)

    # load district to postgresql
    table_name = "dim_case"
    df_status.to_sql(table_name, pg_engine, if_exists='replace', index=False, index_label='id')
    print("============ Success to load case data into PostgreSQL ============")
    pg_engine.dispose()
