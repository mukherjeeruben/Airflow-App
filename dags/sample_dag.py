from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import create_engine
import app_config

def print_hello():
    from sqlalchemy import create_engine
    return 'Hello world from first Airflow DAG!'

def create_connection():
    '''Create sqlalchemy connection object for mysql'''
    
    user = app_config.SQL_USER
    password = app_config.SQL_PASSWORD
    host = app_config.SQL_HOST
    port = app_config.SQL_PORT
    database = app_config.SQL_DATABASE
    print('Variables retrived from config')
    ssl_args = {'ssl_ca': app_config.ca_path}
    print('ca certificate')
    db_string = "mysql+pymysql://" + user + ":" + password + "@" + host + ":" + port + "/" + database
    print(db_string)
    try:
        db = create_engine(db_string, connect_args=ssl_args)
        print('Database Engine Created')
        conn_obj = db.connect()
        print('Database Connection Object Created')
        # return conn_obj
        print(conn_obj)
    except Exception as exp_msg:
        print("Database Connection error: " + str(exp_msg))

dag = DAG('planet_scale_connection', description='DB Connection DAG',
          schedule_interval='0 12 * * *',
          start_date=datetime(2017, 3, 20), catchup=False)

hello_operator = PythonOperator(task_id='db_con_task', python_callable=create_connection, dag=dag)

hello_operator