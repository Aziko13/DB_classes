import TaskConfig as TaskConfig

import pyodbc
from impala.util import as_pandas
import numpy as np
from importlib import reload
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta

reload(TaskConfig)



class ImpalaConnection():
    '''
    Class to manage Impala connection. Initialization is done through TaskConfig file.
    Has execute() and loadTableIntoImpala() methods.
    '''
    def __init__(self, settings=TaskConfig.impala_connection_settings):

        self.conn = pyodbc.connect("""DRIVER={Cloudera ODBC Driver for Impala};
        UID={"""+settings['uid']+"""};
        UseSQLUnicodeTypes=0;
        UseSASL="""+settings['sasl']+""";
        UseOnlySSPI_DSN-less=0;
        UseOnlySSPI=0;
        UseNativeQuery=0;
        UseKeytab=0;
        TSaslTransportBufSize=1000;
        StringColumnLength=32767;
        SSL=0;SocketTimeout=60;RowsFetchedPerBlock=1000;
        Port="""+settings['port']+""";

        Host="""+settings['host']+""";
        PWD="""+settings['pwd']+""";
        EnableSimulatedTransactions=0;AuthMech=3""",
        autocommit=True)

        # -------------------------------------------------------------------------
        '''
        Workaround for older version of Linux
        Had to specify ecnoding before. After re-installation of servers works well without this part 
        '''

        # CHARSET = UTF8
        # self.conn.setdecoding(pyodbc.SQL_CHAR, encoding='utf-8')
        # self.conn.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-8')
        # self.conn.setdecoding(pyodbc.SQL_WMETADATA, encoding='utf-16')

        # if os.name == 'posix':
        #     self.conn.setdecoding(pyodbc.SQL_WMETADATA, encoding='utf-32le')

        # self.conn.setencoding(encoding='utf-8')
        # -------------------------------------------------------------------------

        self.cur = self.conn.cursor()

    def execute(self, query, n=10):
        '''
        Executes Impala query.
        :param query: Impala query in a String format
        :param n: number of attempts to reach a result
        :return: updates self.cur. Use as_pandas(class.cur) to get a pands DataFrame
        '''
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
                impala_types.append('DOUBLE')
            elif "int" in str(dtype):
                impala_types.append('DOUBLE')
            elif "datetime64" in str(dtype):
                impala_types.append('TIMESTAMP')
            else:
                impala_types.append('STRING')
        return impala_types

    def _createTable(self, df, table, partition='', print_q=False):

        self.execute('drop table if exists ' + table)
        columns = ', '.join([name + ' ' + impala_type for name, impala_type in zip(df.columns, self._getTypeDataSet(df))])
        query = """
                    CREATE TABLE """ + table + """ (""" + columns + """) """ + partition + """ STORED AS PARQUET;
                """
        if print_q: print(query)
        self.execute(query)

    def loadTableIntoImpala(self, df, table, partition='', replace=True, print_q=False):
        '''
        Method uploads pandas DataFrame into Impala stored as parqued. First it forms string query from colums and sends it as a big string to Impala.
        :param df: pandas DataFrame
        :param table: table name to create in Impala. Should be specified with schemas i.e. shema.table_name
        :param partition: Optional. To create partitions be column.
        :param replace: Boolean, default=True. If table exists and replace=False then loading is done as input. Otherwise table will be dropped.
        :param print_q: Boolean, default=False. Show or not the final query.
        :return: None. Result will be stored in Impala.
        '''
        def transform_element(el):
            if el is None:
                el = 'Null'
            elif isinstance(el, (bool, int, float, np.int64, np.int32, np.int16, np.int)):
                if np.isnan(el):
                    el = 'Null'
                else:
                    el = str(el)
            else:
                el = "'" + str(el).replace("'", "") + "'"
            return el

        if replace:
            self._createTable(df, table, partition=partition, print_q=print_q)


        columns = ",".join(df.columns)

        values = ",".join(["(" + ",".join([transform_element(el) for el in r[1].values]) + ")" for r in df.iterrows()])

        query = """INSERT INTO """ + table + """ (""" + columns + """) """ + partition + """ values """ + values

        if print_q: print(query)

        self.execute(query)


if __name__ == '__main__':

    # Checking script for connection and it's functions

    # Create Connection to Impala and extracting catalog for test
    conn = ImpalaConnection()
    
    # in the query below table_name has to be replaced to some existing table
    query = '''
            select * from table_name limit 10
            '''
    
    conn.execute(query)

    res = as_pandas(conn.cur)
    print(res.head(3))
    print('Table is loaded')

    # Loading Table into Impala
    # in the query below schema_name has to be replaced to some existing schema
    conn.loadTableIntoImpala(res, table='schema_name.test_table', replace=True, print_q=False)


    # Deleting created table
    conn.execute('drop table if exists schema_name.test_table')

    print('Done')

