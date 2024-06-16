# Import libraries required for connecting to mysql
import mysql.connector

# Import libraries required for connecting to PostgreSql
import psycopg2

# Connect to MySQL
mysql_conn = mysql.connector.connect(user='root',
                                    password='d9ItfB2EcW3mS0F5SQW9l8UU',
                                    host='172.21.151.220',
                                    database='sales')

mysql_cursor = mysql_conn.cursor()


# Connect to PostgreSql
dsn_hostname = '127.0.0.1'
dsn_user='postgres'        # e.g. "abc12345"
dsn_pwd ='MTcwODgta3lvc3Np'      # e.g. "7dBZ3wWt9XN6$o0J"
dsn_port ="5432"                # e.g. "50000" 
dsn_database ="postgres"           # i.e. "BLUDB"

psql_conn = psycopg2.connect(database=dsn_database, 
                            user=dsn_user,
                            password=dsn_pwd,
                            host=dsn_hostname, 
                            port= dsn_port)

psql_cursor = psql_conn.cursor()


# Find out the last rowid from PostgreSql data warehouse
'''The function get_last_rowid must return the last rowid of the table sales_data on PostgreSql.'''

def get_last_rowid():

    psql_cursor.execute('SELECT MAX(row_id) FROM sales_data')
    max_rowid = psql_cursor.fetchall()
    psql_conn.commit()
    
    return max_rowid[0][0]


last_row_id = get_last_rowid()
print("Last row id on production datawarehouse = ", last_row_id)

# List out all records in MySQL database with rowid greater than the one on the Data warehouse
''' The function get_latest_records must return a list of all records that have a rowid greater than
the last_row_id in the sales_data table in the sales database on the MySQL staging data warehouse.'''

def get_latest_records(rowid):

    SQL = f'''SELECT * FROM sales_data WHERE rowid > {rowid}'''
    mysql_cursor.execute(SQL)
    new_recs = mysql_cursor.fetchall()
    
    return new_recs


new_records = get_latest_records(last_row_id)
print("New rows on staging datawarehouse = ", len(new_records))

# Insert the additional records from MySQL into DB2 or PostgreSql data warehouse.
'''The function insert_records must insert all the records passed to it into the sales_data 
table in  PostgreSql.'''

def insert_records(records):
    for r in records:
        SQL = f'''INSERT INTO public.sales_data (row_id, product_id, customer_id, quantity) VALUES {r}'''
        psql_cursor.execute(SQL)
        psql_conn.commit()
        

insert_records(new_records)
print("New rows inserted into production datawarehouse = ", len(new_records))

# disconnect from mysql warehouse
mysql_conn.close()

# disconnect from PostgreSql data warehouse 
psql_conn.close()

# End of program