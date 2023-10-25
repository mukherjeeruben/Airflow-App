from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from operators.bucket_to_sql_operator import bucket_to_sql_operator
from operators.start_operator import start_operator

default_args = {
    'owner': 'batmachine'
}


with DAG(
    'speed_load',
    default_args=default_args,
    concurrency = 6,
    catchup = False,
    max_active_runs = 1,
    schedule_interval = '*/30 10-11 * * 1-5',
    start_date = days_ago(0),
    tags=['de_core', 'data_pull', 'data_push',]
) as dag:
    
    start = start_operator(
        task_id='start_speed_load',
        run_id=''
    )

    with TaskGroup("buckettosql", tooltip='buckettosql') as buckettosql:
        bucket_sql = bucket_to_sql_operator(
            task_id = "data_pull",
            sql = "select * from orderdetails"
        )

    [bucket_sql]


    end = DummyOperator(task_id = 'end')

    start >> buckettosql >> end