from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.models.xcom import XCom


class CustomPythonOperator(BaseOperator):
    @apply_defaults
    def __init__(self,
                 operation_type='',
                 *args, **kwargs):
        self.operation_type = operation_type
        super(CustomPythonOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        parameter1 = context['params']['parameter1']
        parameter2 = context['params']['parameter2']

        if all(parameter1.values()) and self.operation_type == 'table1':
            print(parameter1)
        elif all(parameter2.values()) and self.operation_type == 'table2':
            print(parameter2)
