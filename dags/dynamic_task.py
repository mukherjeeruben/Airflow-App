from airflow.decorators import dag
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator

DAG_ID = "s3_file_debug"


def print_parameters():
    print('Task Executed')

@dag(
    dag_id = DAG_ID,
    start_date=days_ago(0),
    schedule_interval=None
)
def create_dag(**context):
    start_debug = DummyOperator(task_id='start_debug')

    # source_table = context['dag_run'].conf['source_table']
    source_table = 'hello,world,hola,he'
    for source in source_table.split(','):
        with TaskGroup(group_id=source) as dummy_task:
            d_task = PythonOperator(task_id='d_task',
                                    python_callable=print_parameters)

            [d_task]


        start_debug >> dummy_task


globals()[DAG_ID] = create_dag()
