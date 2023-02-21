from utils.query_execution import execute_query


if __name__ == '__main__':
    query = '''SELECT * FROM Customers'''
    result = execute_query(query=query)
    print(result)


