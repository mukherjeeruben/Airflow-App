from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.models.xcom import XCom


class CustomIntertaskPushOperator(BaseOperator):
    @apply_defaults
    def __init__(self,
                 operation_type='',
                 *args, **kwargs):
        self.operation_type = operation_type
        super(CustomIntertaskPushOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        context['ti'].xcom_push(key='key1', value='default_value')