


# --------------------------------------------------------------------------------
# --------------------------- DB Connections Settings ----------------------------

impala_connection_settings = {
                                'host':'host_name',
                                'sasl':'1',
                                'uid':'user_id',
                                'pwd':'password',
                                'port':'21050'
                            }

mysql_connection_settings = {
                                'driverClassName':'com.mysql.jdbc.Driver',
                                'urljdbc': 'jdbc:mysql://',
                                'host': 'host_name',
                                'database': 'adv',
                                'username': 'user_id',
                                'password': 'password',
                                'suppressClose': 'true',
                                'autoCommit': 'true'
                            }

oracle_connection_settings = {
                            'host':'host_name',
                            'port':'1521',
                            'service_name':'service_name',
                            'user':r'user_id',
                            'password':'password'
                            }
# --------------------------------------------------------------------------------
# --------------------------------------------------------------------------------

