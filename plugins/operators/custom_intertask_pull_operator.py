from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.models.xcom import XCom


class CustomIntertaskPullOperator(BaseOperator):
    @apply_defaults
    def __init__(self,
                 operation_type='',
                 *args, **kwargs):
        self.operation_type = operation_type
        super(CustomIntertaskPullOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        fetch_key = context['ti'].xcom_pull(key='key1')
        print(fetch_key)