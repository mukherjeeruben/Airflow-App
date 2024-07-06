from sqlalchemy import create_engine, text

class sql_config:
    def __init__(self):
        self.SQL_SPECS ={"SQL_DATABASE" : "",
            "SQL_HOST" : "",
            "SQL_PORT" : "",
            "SQL_USER" : "",
            "SQL_PASSWORD" : "",
            "ca_path" : "/etc/ssl/certs/ca-certificates.crt"}


class sqlHook(sql_config):
    def create_connection(self):
        '''Create sqlalchemy connection object for mysql (planetscale)'''
        user = self.SQL_SPECS["SQL_USER"]
        password = self.SQL_SPECS["SQL_PASSWORD"]

        host = self.SQL_SPECS["SQL_HOST"]
        port = self.SQL_SPECS["SQL_PORT"]
        database = self.SQL_SPECS["SQL_DATABASE"]
        ssl_args = {'ssl_ca': self.SQL_SPECS["ca_path"]}
        db_string = "mysql+pymysql://" + user + ":" + password + "@" + host + ":" + port + "/" + database

        try:
            db = create_engine(db_string, connect_args=ssl_args)
            conn_obj = db.connect()
            print('Database Connection Object Created')
            return conn_obj
        except Exception as exp_msg:
            print("Database Connection error: " + str(exp_msg))