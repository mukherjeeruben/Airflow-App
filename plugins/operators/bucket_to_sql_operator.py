from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helper.execute_query import ExecuteQuery


class bucket_to_sql_operator(BaseOperator):
    @apply_defaults
    def __init__(self, 
                 sql='',
                 *args, **kwargs):
        self.sql = sql
        super(bucket_to_sql_operator, self).__init__(*args, **kwargs)

    def execute(self, context, **kwargs):
        print(context['dag_run'].conf['exec_date'])
        sql_execute = ExecuteQuery()
        results = sql_execute.execute(query=self.sql)
        print(results)