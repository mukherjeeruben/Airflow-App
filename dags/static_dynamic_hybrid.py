from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'ruben'
}


def print_parameters(**context):
    pass


with DAG(
        'static_dynamic_hybrid',
        default_args=default_args,
        concurrency=6,
        catchup=False,
        max_active_runs=1,
        schedule_interval=None,
        start_date=days_ago(0),
        params={"gaps_to_ignore": {'values': ""},
                "s3_debug": {'tables': "", 'start_date': "", 'drop_table': "False"}},
        tags=['dynamic', 'hybrid', 'static']
) as dag:
    start = DummyOperator(task_id='start')

    with TaskGroup("statictask", tooltip='statictask') as statictaskgroup:
        # static_task = PythonOperator(task_id='db_con_task',
        #                         python_callable=print_parameters,
        #                         op_kwargs={'type': 'statictask'})

        static_task = PythonOperator(task_id='db_con_task',
                                python_callable=print_parameters)

    [static_task]

    with TaskGroup("dynamictask", tooltip='dynamictask') as dynamictaskgroup:
        for dynamic_task_item in ['a', 'b', 'c', 'd', 'e']:
            dynamic_task = PythonOperator(task_id='dynamic_task_'+dynamic_task_item, python_callable=print_parameters)
        [dynamic_task]

    end = DummyOperator(task_id='end')

    start >> statictaskgroup >> dynamictaskgroup >> end