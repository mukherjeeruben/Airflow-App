from hooks.sql_hook import sqlHook, text
import sqlparse


class ExecuteQuery:
    def sql_query_type(self, query_string):
        '''Get Query Type'''
        query_type = str(sqlparse.parse(query_string)[0].get_type()).upper()
        return query_type

    def execute(self, query, query_params=None):
        sqlhookobj = sqlHook()
        try:
            query_type = self.sql_query_type(query_string=query)
            with sqlhookobj.create_connection() as conn:
                if query_params == None:
                    cursor = conn.execute(text(query))
                else:
                    cursor = conn.execute(text(query), query_params)
                print('Query Executed')
                if query_type == 'SELECT':
                    columns = list(map(lambda x: x[0], cursor.cursor.description))
                    row_vaules = cursor.fetchall()
                    result_set = [dict(zip(columns, row)) for row in row_vaules]
                elif query_type == 'INSERT' or query_type == 'UPDATE' or query_type == 'DELETE':
                    result_set = cursor.rowcount
                elif query_type == 'ALTER':
                    return 'Altered'
                conn.close()
                return result_set
        except Exception as exp_msg:
            print("Error in Query Execution: " + str(exp_msg))


