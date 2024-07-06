from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from operators.pytest_operator import PyTestOperator
from operators.start_operator import start_operator

default_args = {
    'owner': 'ruben'
}


with DAG(
    'pytest_automation',
    default_args=default_args,
    concurrency=1,
    catchup=False,
    max_active_runs=1,
    schedule_interval=None,
    start_date=days_ago(0),
    tags=['pytest', 'automation_testing'],
) as dag:
    
    start = start_operator(
        task_id='start_automation',
        do_xcom_push=True,
        dag_name='pytest_automation'
    )

    with TaskGroup("automationtaskgroup", tooltip='automationtaskgroup') as automationtaskgroup:
        automation_task = PyTestOperator(
            task_id="automation_testing"
        )

    [automation_task]

    end = DummyOperator(task_id='end')

    start >> automationtaskgroup >> end