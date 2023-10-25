from sqlalchemy import create_engine, text
# from config.app_config import SQL_SPECS

SQL_SPECS ={"SQL_DATABASE" : "airflowpoc",
            "SQL_HOST" : "aws.connect.psdb.cloud",
            "SQL_PORT" : "3306",
            "SQL_USER" : "cwbcsa2kssyxfgep4xeh",
            "SQL_PASSWORD" : "pscale_pw_eDRDmeMCwboQPBGankrf6eqk6E9HEcgNwkY4I2LV1KK",
            "ca_path" : "/etc/ssl/certs/ca-certificates.crt"}

class sqlHook:
    def create_connection(self):
        '''Create sqlalchemy connection object for mysql'''
        user = SQL_SPECS["SQL_USER"]
        password = SQL_SPECS["SQL_PASSWORD"]

        host = SQL_SPECS["SQL_HOST"]
        port = SQL_SPECS["SQL_PORT"]
        database = SQL_SPECS["SQL_DATABASE"]
        ssl_args = {'ssl_ca': SQL_SPECS["ca_path"]}
        db_string = "mysql+pymysql://" + user + ":" + password + "@" + host + ":" + port + "/" + database

        try:
            db = create_engine(db_string, connect_args=ssl_args)
            conn_obj = db.connect()
            print('Database Connection Object Created')
            return conn_obj
        except Exception as exp_msg:
            print("Database Connection error: " + str(exp_msg))