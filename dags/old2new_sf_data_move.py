from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator


default_args = {"owner":"Marketexperts",
"start_date":datetime(2022,7,28,6)

}
sf_read_query= """
"""
with DAG(dag_id="Data4old2newSF",
schedule_interval='@hourly',
start_date=datetime(2022,7,28,18),
end_date=None,
max_active_runs= 5,
default_args=default_args,
description="data move from old to new sf") as dag:
    task_1 = SnowflakeOperator(
        task_id = "read_sf_data",
        sql = sf_read_query,
        snowflake_conn_id= "snowflake_conn_id_2"
    )