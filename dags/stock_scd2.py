from airflow import DAG
from datetime import datetime
import airflow
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.operators.python_operator import PythonOperator

int_stg_tab_stg_q = """ COPY into EDW_DEV.EDW.STOCK_DIM_STG from (select $1,$2,$3,$4,$5,metadata$filename from @EDW_DEV.EDW.NSE_DAILY_STG)
 pattern= '.*ind_nifty.*'  file_format = 'EDW_DEV.EDW.NSE_SD_FMT_CSV' purge =true;"""
stg_2_tgt_mer_q = """ MERGE into EDW_DEV.EDW.STOCK_DIM sd
using EDW_DEV.EDW.STOCK_DIM_STG_STRM sdstr on sd.ISIN_CODE = sdstr.ISIN_CODE
when not matched and sdstr.METADATA$ACTION ='INSERT' and sdstr.METADATA$ISUPDATE = FALSE 
then
insert (sd.COMPANY_NAME,sd.INDUSTRY,sd.SYMBOL,sd.SERIES,sd.ISIN_code,sd.INDEX_NAME)  
values (sdstr.COMPANY_NAME,sdstr.INDUSTRY,sdstr.SYMBOL,sdstr.SERIES,sdstr.ISIN_code, substr(sdstr.INDEX_NAME,1,13))
"""
default_args = {"owner": "NSEstock", "start_date": datetime(2022,8,1)}
dags =DAG(dag_id="Stockdim_update",
          default_args=default_args,
          schedule_interval=None
)
def load_file_lcl_name():
    lcl_2_stg = SnowflakeHook(snowflake_conn_id ="snowflake_conn_id")
    sql_t =(f"put file:///home/osboxes/Desktop/NSE_OF/ind_nifty*.csv @EDW_DEV.EDW.NSE_DAILY_STG; ")
    res1 = lcl_2_stg.run(sql=sql_t)


with dags:
    file_ftp_named_stg = PythonOperator(
        task_id = "load_ftp_stage",
        python_callable=load_file_lcl_name
    )
    named_stg_tab_stg =SnowflakeOperator(
        task_id =  "named_stg_tab_stg_l",
        sql = int_stg_tab_stg_q,
        snowflake_conn_id= "snowflake_conn_id"
     )
    stg_tgt_merg_sd = SnowflakeOperator(
        task_id = "stg_2_tgt_merge_sd",
        sql = stg_2_tgt_mer_q,
        snowflake_conn_id = "snowflake_conn_id"
    )


    file_ftp_named_stg >> named_stg_tab_stg >> stg_tgt_merg_sd