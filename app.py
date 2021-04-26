from cassandra.cluster import Cluster
# from cassandra.auth import PlainTextAuthProvider

"""
PlainTextAuthProvider was commented out to remove the authentication restrictions
and make the script more accessible during development
""" 
# auth_provider = PlainTextAuthProvider(username='user', password='pass')
# cluster = Cluster(auth_provider=auth_provider)


"""
Creating a cluster object and connection session to the KEYSPACE.
""" 
cluster=Cluster()
session = cluster.connect('salmandatamodel')



"""
This initial function Drops all entities, employee and salary, 
create them and populate a row in both tables.
"""
def init():
    session.execute("DROP TABLE IF EXISTS employee")
    session.execute("DROP TABLE IF EXISTS salary")

    session.execute(
    """
    CREATE TABLE employee (
    employee_id int PRIMARY KEY, 
    employee_name text, 
    department_name text,
    position text,
    company_name text);
    """
    )
    session.execute(
    """
    CREATE TABLE salary (
    employee_id int PRIMARY KEY, 
    base_salary int, 
    medical_allowance int,
    travel_allowance int,
    total_salary int);
    """
    )
    

    session.execute(
    """
    INSERT INTO employee (employee_id, employee_name, department_name, position, company_name) 
    VALUES (0, 'rema salman', 'Sales', 'HR', 'IKEA');
    """ 
    )

    session.execute(
    """
    INSERT INTO salary (employee_id, base_salary, medical_allowance, travel_allowance, total_salary) 
    VALUES (0, 4000, 4000, 4000, 4000);
    """ 
    )



"""
This function updates the recods, employee and salary 
according to the passed parameters
""" 
def updateRecords(employee_id,name,salary): 
    session.execute(
    """
    UPDATE employee SET employee_name=%s WHERE employee_id=%s;
    """ ,
    (name,employee_id)
    )
    session.execute(
    """
    UPDATE salary SET total_salary=%s WHERE employee_id=%s;
    """ ,
    (salary,employee_id)
    )



"""
The Main Script execute the initial function, prints the records,
evokes updateRecords function, and prints the modified records.
""" 
init()
rows = session.execute('SELECT * FROM employee')
for user_row in rows:
    print(user_row)

rows = session.execute('SELECT * FROM salary')
for user_row in rows:
    print(user_row)

updateRecords(0,"Rema Salman",4500)

rows = session.execute('SELECT * FROM employee')
for user_row in rows:
    print(user_row)

rows = session.execute('SELECT * FROM salary')
for user_row in rows:
    print(user_row)