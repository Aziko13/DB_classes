# Data base connectors

Project contains classess to work with databases (Impala/Oracle/MySQL) from python/Windows.
In order to get an access to data bases one needs to install approrpiate drivers and libraries.
As an example you can walk through the project about [Impala connector][https://github.com/Aziko13/ImpalaODBC]
**Classes** have methods like create table/load table and execute a query.
**TaskConfig.py** module contains connections' description for all databases. One have to fill them out.

## Getting Started



### Prerequisites

### Installing

* Python 3.6.7
* pyodbc 4.0.22 
* impyla 0.14.1
* numpy
* dateutil

### Example
```
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

```
	

## Authors

* **Aziz Abdraimov** - *Initial work* - [Aziko13](https://github.com/Aziko13)

