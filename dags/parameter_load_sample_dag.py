from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from operators.custom_python_operator import CustomPythonOperator
from operators.start_operator import start_operator

default_args = {
    'owner': 'ruben'
}


with DAG(
    'parameter_load_sample_dag',
    default_args=default_args,
    concurrency=6,
    catchup=False,
    max_active_runs=1,
    schedule_interval='*/30 10-11 * * 1-5',
    start_date=days_ago(0),
    tags=['de_core', 'data_pull', 'data_push'],
    params={"parameter1": {"key1": "", "key2": ""},
            "parameter2": {"key1": "", "key2": ""}}
) as dag:
    
    start = start_operator(
        task_id='start_load',
        do_xcom_push=True,
        dag_name='start_load_task'
    )

    with TaskGroup("buckettosql", tooltip='buckettosql') as buckettosql:
        cdc_table1 = CustomPythonOperator(
            task_id="data_pull_cdc_table1",
            operation_type='table1',
        )
        cdc_table2 = CustomPythonOperator(
            task_id="data_pull_cdc_table2",
            operation_type='table2',

        )


    [cdc_table1, cdc_table2]

    end = DummyOperator(task_id='end')

    start >> buckettosql >> end