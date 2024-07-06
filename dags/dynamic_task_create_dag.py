from airflow.decorators import dag
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator

DAG_ID = "dynamic_task_dag"

def print_parameters(**context):
    print(context['params']['parameter_1']['key1'])
    print(context['params']['parameter_1']['key2'])
    print(context['params']['parameter_1']['key3'])
    print(context['params']['parameter_1']['key4'])
    print(context['params']['parameter_1']['key5'])
    print(context['params']['parameter_2']['key1'])
    print(context['params']['parameter_2']['key2'])
    print(context['params']['parameter_2']['key3'])
    print('Task Executed')

@dag(
    dag_id=DAG_ID,
    start_date=days_ago(0),
    params={"parameter_1": {'key1': "", 'key2': "", 'key3': "", 'key4': "", 'key5': ""},
            "parameter_2": {'key1': "", 'key2': "", 'key3': ""}},
    schedule_interval=None
)
def create_dag(**context):
    start_debug = DummyOperator(task_id='start_debug')
    source_table = 'task1,task2,task3,task4'
    for source in source_table.split(','):
        with TaskGroup(group_id=source) as dynamic_task:
            d_task = PythonOperator(task_id='d_task',
                                    python_callable=print_parameters)

            d_task

    with TaskGroup(group_id='dummy_task_s') as dummy_task_s:
        s_task = PythonOperator(task_id='s_task',
                                python_callable=print_parameters)

    start_debug >> dummy_task_s >> dynamic_task


globals()[DAG_ID] = create_dag()
