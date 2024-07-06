from airflow.models import BaseOperator
from helper.execute_query import ExecuteQuery
from airflow.utils.decorators import apply_defaults
from datetime import datetime
from airflow.models.xcom import XCom


class start_operator(BaseOperator):
    @apply_defaults
    def __init__(self, 
                 dag_name='',
                 *args, **kwargs):
        self.dag_name = dag_name
        super(start_operator, self).__init__(*args, **kwargs)

    def execute(self, context):
        sql_hook = ExecuteQuery()
        dag_run_id = context['run_id']
        print(str(dag_run_id))

        start_time = datetime.utcnow()
        sql = '''INSERT INTO DAG_RUNS
                (DAG_RUN_ID, DAG_NAME, START_TIME, CREATE_DATE)
                VALUES
                ('{}','{}','{}','{}')'''.format(dag_run_id, self.dag_name, start_time, start_time)
        sql_hook.execute(query=sql)
