import journal.TaskConfig as TC

# import mysql.connector
from impala.util import as_pandas
import numpy as np
import pyodbc
import os


class MySQLConnection():


    def __init__(self, settings = TC.mysql_connection_settings):

        if os.name == 'posix':
            driver_name = 'Devart ODBC Driver for MySQL'
        else:
            driver_name = 'MySQL ODBC 8.0 ANSI Driver'

        self.conn = pyodbc.connect('''
                                DRIVER={'''+driver_name+'''};
                                Server=''' + settings['host'] + ''';
                                User='''+settings['username']+''';
                                Password=''' + settings['password'] + ''';
                                OPTION=3
                                    ''')

        self.cur = self.conn.cursor()

    def execute(self, query, n=10):

        for i in range(n):
            try:
                self.cur.execute(query)
            except Exception:
                if i == (n-1):
                    print("Query execution error")
                pass
            else:
                break

    def _getTypeDataSet(self, df):
        impala_types = []
        for name, dtype in zip(df.columns, df.dtypes):
            if "bool" in str(dtype):
                impala_types.append('BOOLEAN')
            elif "float" in str(dtype):
                impala_types.append('BIGINT')
            elif "int" in str(dtype):
                impala_types.append('BIGINT')
            elif "datetime64" in str(dtype):
                impala_types.append('TIMESTAMP')
            else:
                impala_types.append('VARCHAR(100)')
        return impala_types

    def _createTable(self, df, table, print_q=False):

        self.execute('drop table if exists ' + table)
        columns = ', '.join([name + ' ' + impala_type for name, impala_type in zip(df.columns, self._getTypeDataSet(df))])
        query = """
                    CREATE TABLE """ + table + """ (""" + columns + """) ENGINE=MyISAM AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 ;
                """

        if print_q: print(query)
        self.execute(query)

    def loadTableIntoMySQL(self, df, table, partition='', replace=True, print_q=False):

        def transform_element(el):
            if el is None:
                el = 'NULL'
            elif isinstance(el, (bool, int, float, np.int64, np.int32, np.int16, np.int)):
                if np.isnan(el):
                    el = 'NULL'
                else:
                    el = str(el)
            else:
                el = "'" + str(el).replace("'", "") + "'"
            return el

        if replace:
            self._createTable(df, table, print_q=print_q)


        columns = ",".join(df.columns)

        values = ",".join(["(" + ",".join([transform_element(el) for el in r[1].values]) + ")" for r in df.iterrows()])

        query = """INSERT INTO """ + table + """ (""" + columns + """) """ + partition + """ values """ + values

        if print_q: print(query)

        self.execute(query)


def main():
    # Loading data from MySQL as an example
    conn = MySQLConnection()
    # in the query below table_name has to be replaced to some existing table
    query = '''
            select * from table_name limit 10
            '''
    conn.execute(query)

    res = as_pandas(conn.cur)
    print(res.head())

    res.loc[res.shape[0]] = [None, 'TEST']

    print('Table is loaded')

    # Loading Table into MySQL (another scheme)
    # in the query below schema_name has to be replaced to some existing schema
    conn.loadTableIntoMySQL(res, table='schema_name.test_table', replace=True, print_q=False)

    # Deleting created table
    conn.execute('drop table if exists schema_name.test_table')

    print('Done')

if __name__ == '__main__':

    main()

