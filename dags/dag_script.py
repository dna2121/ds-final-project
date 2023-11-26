from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from datetime import datetime
from fun.fun_get_data_from_api import fun_get_data_from_api
from fun.fun_generate_dim import fun_generate_dim
from fun.fun_insert_district_daily import fun_insert_district_daily

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

    op_generate_dim = PythonOperator(
        task_id='generate_dim',
        python_callable=fun_generate_dim
    )

    op_insert_district_daily = PythonOperator(
        task_id='insert_district_daily',
        python_callable=fun_insert_district_daily
    )

    op_insert_province_daily = EmptyOperator(
        task_id='insert_province_daily'
    )

    #flow in dag
    op_get_data_from_api >> op_generate_dim >> [op_insert_district_daily, op_insert_province_daily]

