from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from operators.custom_intertask_push_operator import CustomIntertaskPushOperator
from operators.custom_intertask_pull_operator import CustomIntertaskPullOperator

default_args = {
    'owner': 'ruben'
}


with DAG(
        'context_variable',
        default_args=default_args,
        concurrency=6,
        catchup=False,
        max_active_runs=1,
        schedule_interval=None,
        start_date=days_ago(0),
        tags=['variable', 'push', 'pull']
) as dag:
    start = DummyOperator(task_id='start')

    with TaskGroup("variablepushtask", tooltip='variablepushtask') as variablepushtaskgroup:
        variable_push_task = CustomIntertaskPushOperator(task_id='taskpush')

    [variable_push_task]

    with TaskGroup("variablepulltask", tooltip='variablepulltask') as variablepulltaskgroup:
        variable_pull_task = CustomIntertaskPullOperator(task_id='taskpull')

    [variable_pull_task]

    end = DummyOperator(task_id='end')

    start >> variablepushtaskgroup >> variablepulltaskgroup >> end