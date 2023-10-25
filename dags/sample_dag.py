from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


def print_parameters(**context):
    print(context['dag_run'].conf['exec_date'])



dag = DAG('planet_scale_connection', description='DB Connection DAG',
          schedule_interval='0 12 * * *',
          start_date=datetime(2017, 3, 20), catchup=False)

hello_operator = PythonOperator(task_id='db_con_task', python_callable=print_parameters, dag=dag)

hello_operator