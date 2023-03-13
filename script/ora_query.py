# query.py

import cx_Oracle

# Establish the database connection
# connection = cx_Oracle.connect("system", "oracle", "dbhost.example.com/orclpdb1")
connection = cx_Oracle.connect("ETLADM", "ETLADM", "172.16.99.130/DGODPD")


# Obtain a cursor
cursor = connection.cursor()

# Data for binding
managerId = 145
firstName = "Peter"

# Execute the query
# sql = """SELECT first_name, last_name
#          FROM hr.employees
#          WHERE manager_id = :mid AND first_name = :fn"""
# cursor.execute(sql, mid = managerId, fn = firstName)
sql = "SELECT count(*) FROM ods.FND_COLUMNS "
cursor.execute(sql)

# Loop over the result set
for row in cursor:
    print(row)