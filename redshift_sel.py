import time, psycopg2, os, sys
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

REDSHIFT_CONNECT_STRING = "dbname='dev' port='5439' user='masteruser' password='5' host='mytestcluster.cnwamy.us-west-1.redshift.amazonaws.com'"

# os.environ['REDSHIFT_CONNECT_STRING']
AWS_ACCESS_KEY_ID = 'sss'
# os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = 'dfg'
# os.environ['AWS_SECRET_ACCESS_KEY']
AWS_IAM_ROLE ='arn:aws:iam::376621:role/Newredshiftrole'

#Lets try to connect to cluster
conn_string = REDSHIFT_CONNECT_STRING.strip().strip('"')
try:    
    con = psycopg2.connect(conn_string, async=0)
except psycopg2.Error, e:
    print e.diag.severity
    print e.diag.message_primary
    print("Unable to connect to %s!" % REDSHIFT_CONNECT_STRING)
    

con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
cur = con.cursor()
#cur.execute("select version()")
#rows = cur.fetchall()
#for entry in rows:
 #   print("Connect sucessfull! Version of the cluster is %s " % entry)
#    https://plot.ly/python/amazon-redshift/
aws_key =''
    #os.getenv("AWS_ACCESS_KEY_ID") # needed to access S3 Sample Data
aws_secret ='' 
    #os.getenv("AWS_SECRET_ACCESS_KEY")

   
#base_copy_string = """copy %s from 's3://albabucket/retail_db/%s.csv' credentials 'AWS_IAM_ROLE=%s' delimiter '%s';""" # the base COPY string 
base_copy_string = """copy %s from 's3://albabucket/retail_db/%s.csv' credentials 'aws_access_key_id=%s;aws_secret_access_key=%s' ACCEPTANYDATE ACCEPTINVCHARS REMOVEQUOTES TRUNCATECOLUMNS delimiter '%s';""" 
#easily generate each table that we'll need to COPY data from
tables = ["order_items", "orders", "categories", "products", "departments"]
data_files = ["order_items", "orders", "categories", "products", "departments"]
delimiters = [",", ",", ",", ",", ","]

#the generated COPY statements 
copy_statements = []
for tabl, fil, delim in zip(tables, data_files, delimiters):
    copy_statements.append(base_copy_string % (tabl, fil, aws_key, aws_secret, delim))

'''
print ("Creating tables ")  


try:  
    cur.execute("""
     CREATE TABLE departments (
     department_id int NOT NULL,
     department_name varchar(45) NOT NULL);
     
     CREATE TABLE order_items (
     order_item_id int NOT NULL,
     order_item_order_id int NOT NULL,
     order_item_product_id int NOT NULL,
     order_item_quantity int NOT NULL,
     order_item_subtotal float NOT NULL,
     order_item_product_price float NOT NULL);
  
     CREATE TABLE orders (
     order_id int NOT NULL,
     order_date timestamp NOT NULL,
     order_customer_id int NOT NULL,
     order_status varchar(45) NOT NULL);
     
     CREATE TABLE categories (
     category_id int NOT NULL,
     category_department_id int NOT NULL,
     category_name varchar(45) NOT NULL);

     CREATE TABLE products (
     product_id int NOT NULL,
     product_category_id int NOT NULL,
     product_name varchar(55) NOT NULL,
     product_description varchar(255) NOT NULL,
     product_price float NOT NULL,
     product_image varchar(255) NOT NULL);
     """)
except psycopg2.Error, e:
    print e.diag.severity
    print e.diag.message_primary
    sys.exit(1)
'''    
print ("Loading tables ")  

'''

for copy_statement in copy_statements: # execute each COPY statement
    try:
        cur.execute(copy_statement)   #print(copy_statement)
        
    except psycopg2.Error, e:
        print e.diag.severity
        print e.diag.message_primary
        con.commit()
'''
for table in tables:
    cur.execute("select count(*) from %s;" % (table,))
    print ("select count(*) from %s;" % (table,))
    print(cur.fetchone())
    con.commit()

con.close()
  
