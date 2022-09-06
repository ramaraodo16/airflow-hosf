
from asyncio.log import logger
import logging
from airflow import DAG
import datetime as dt
import airflow
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.operators.bash_operator import BashOperator
logging.basicConfig(level=logging.info)
logger = logging.getLogger(__name__)



args ={"owner": "Airflow","start_date": airflow.utils.dates.days_ago(2)}

dag =DAG(dag_id= "Snowflake_automation", default_args = args, schedule_interval = None)

sf_query = [
    """ Create table EDW_PROD.EDW.Airflow_test_tab IF not exists  (dag_pname string , Pipe_number number(10,0));""",
    """ insert into EDW_PROD.EDW.Airflow_test_tab values('Test_AF_SF', 1);""",
    """ insert into EDW_PROD.EDW.Airflow_test_tab values('Test2_AF_SF', 2);"""
]

def get_row_count(**context):
    dwh_hook = SnowflakeHook(snowflake_conn_id="snowflake_conn_id")
    result1 = dwh_hook.get_first("select count(*) from EDW_PROD.EDW.Airflow_test_tab")
    logging.info("table row count%s",result1[0])
with dag:
        create_insert = SnowflakeOperator(

            task_id = "snowflake_create",
            sql = sf_query,
            snowflake_conn_id= "snowflake_conn_id"
        )
        get_count = PythonOperator(task_id= "get_count", python_callable=get_row_count)
        try:
            create_insert >> get_count
        except:
            logging.info("connecction failed")
