from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from datetime import datetime


class start_operator(BaseOperator):
    @apply_defaults
    def __init__(self, 
                 run_id='',
                 *args, **kwargs):
        self.run_id = run_id
        super(start_operator, self).__init__(*args, **kwargs)

    def execute(self, context):
        self.run_id = context['run_id']
        start_time = datetime.utcnow()
        print('run_id: ' + str(self.run_id))
