FROM apache/airflow:2.7.2

ADD requirements.txt .
RUN pip install -r requirements.txt