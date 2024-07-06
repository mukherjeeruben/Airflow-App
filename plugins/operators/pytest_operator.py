from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import pytest
import os


class PyTestOperator(BaseOperator):
    @apply_defaults
    def __init__(self, 
                 sql='',
                 *args, **kwargs):
        self.sql = sql
        super(PyTestOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        full_path = os.path.realpath(__file__)
        directories = full_path.split('/')
        lst = directories[:len(directories) - 2]
        lst.append('tests')
        path = '/'.join(lst)
        print(path)

        print('*** Automation Testing Start ***')
        logs = LogCollector()
        pytest_args = [
            path,
            # '/opt/airflow/plugins/tests',
            # other tests here...
        ]
        pytest.main(pytest_args, plugins=[logs])

        for report in logs.reports:
            print(report.nodeid, 'outcome:', report.outcome)
        print('passed:', logs.passed, 'failed:', logs.failed, 'skipped:', logs.skipped)


class LogCollector:
    def __init__(self):
        self.reports = []
        self.collected = 0
        self.passed = 0
        self.failed = 0
        self.skipped = 0

    @pytest.hookimpl(hookwrapper=True)
    def pytest_runtest_makereport(self, item, call):
        outcome = yield
        report = outcome.get_result()
        if report.when == 'call':
            self.reports.append(report)

    def pytest_collection_modifyitems(self, items):
        self.collected = len(items)

    def pytest_terminal_summary(self, terminalreporter):
        self.passed = len(terminalreporter.stats.get('passed', []))
        self.failed = len(terminalreporter.stats.get('failed', []))
        self.skipped = len(terminalreporter.stats.get('skipped', []))


