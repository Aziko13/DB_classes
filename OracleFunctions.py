import journal.TaskConfig as TC
import cx_Oracle
from impala.util import as_pandas
import numpy as np


class OracleConnection():

    def __init__(self, settings=TC.oracle_connection_settings):

        self.dsn_tns = cx_Oracle.makedsn(host=settings['host'], port=settings['port'], service_name=settings['service_name'])

        self.conn = cx_Oracle.connect(user=settings['user'], password=settings['password'], dsn=self.dsn_tns)

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
                impala_types.append('NUMBER(1)')
            elif "float" in str(dtype):
                impala_types.append('NUMBER(30)')
            elif "int" in str(dtype):
                impala_types.append('NUMBER(30)')
            elif "datetime64" in str(dtype):
                impala_types.append('VARCHAR(100)')
            else:
                impala_types.append('VARCHAR(100)')
        return impala_types

    def _createTable(self, df, table, partition='', print_q=False):


        self.execute('drop table ' + table)

        columns = ', '.join([name + ' ' + impala_type for name, impala_type in zip(df.columns, self._getTypeDataSet(df))])
        query = """CREATE TABLE """ + table + """ (""" + columns + """) """ + partition

        if print_q:
            print(query)

        self.execute(query)
        self.cur.execute('commit')

    def loadTableIntoOracle(self, df, table, partition='', replace=True, print_q=False):

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
            self._createTable(df, table, partition=partition, print_q=print_q)


        columns = ",".join(df.columns)

        values = " ".join(["INTO " +table+ "("+columns+") values " + "(" + ",".join([transform_element(el) for el in r[1].values]) + ")" for r in df.iterrows()])

        query = """INSERT ALL """ + values + " select * from dual"

        if print_q:
            print(query[0:300])

        self.execute(query)
        self.cur.execute('commit')


def main():
    conn = OracleConnection()
    # in the query below table_name has to be replaced to some existing table
    query = '''
            select * from table_name where rownum <= 5
            '''
    conn.execute(query)

    res = as_pandas(conn.cur)
    print(res.head(3))
    print('Table is here')

    res.loc[res.shape[0]] = [None, 1223, 2232, 3232, 'TEST']
    
    # in the query below schema_name has to be replaced to some existing schema
    conn.loadTableIntoOracle(res, table='schema_name.test_table', replace=True, print_q=False)

    # Deleting created table

    conn.execute('drop table schema_name.test_table')
    conn.execute('commit')

    print('Done')

if __name__ == '__main__':

    main()