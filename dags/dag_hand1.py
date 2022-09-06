from airflow import DAG
from datetime import datetime , timedelta
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "SFAFDE" ,
    "retries": 5 ,
    "retry_delay": timedelta(minutes=2)

}

with DAG(
    dag_id= "Hands_pract_v1",
    default_args = default_args,
    start_date =datetime(2022,7,25,2),
    schedule_interval='@daily'
) as dag:
    task_1 = BashOperator(
    task_id ="first_task",
    bash_command =" echo Hello world this is my first task in this hands on!"


)
task_1