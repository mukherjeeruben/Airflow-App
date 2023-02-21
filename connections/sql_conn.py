import logging
from sqlalchemy import create_engine, text
import config


def create_connection():
    '''Create sqlalchemy connection object for mysql'''
    logging.basicConfig(level=logging.INFO)

    user = config.SQL_USER
    password = config.SQL_PASSWORD
    host = config.SQL_HOST
    port = config.SQL_PORT
    database = config.SQL_DATABASE
    ssl_args = {'ssl_ca': config.ca_path}
    db_string = "mysql+pymysql://" + user + ":" + password + "@" + host + ":" + port + "/" + database

    try:
        db = create_engine(db_string, connect_args=ssl_args)
        logging.info('Database Engine Created')
        conn_obj = db.connect()
        logging.info('Database Connection Object Created')
        return conn_obj
    except Exception as exp_msg:
        logging.error("Database Connection error: " + str(exp_msg))

