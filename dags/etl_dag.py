#!bin/python3

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from mysql.connector import Connect
import pandas as pd


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
}


'''
# 
# Extract Functions 
# 
# 
''' 
def extract_customer_data(**kawrgs):
    conn = Connect(
        host="192.168.43.86",  # your host, usually localhost
        user="root",  # your username
        password="said2002",  # password leave it blank if there isn't any
        port="3306",  # port (3306 default)
        db="ecommercedb", # name of the database
    )  

    pd.read_sql(sql=f"select * from `customer`", con=conn).to_csv("/tmp/extract_customer_data.csv", index=False)


def extract_product_data(**kawrgs):
    conn = Connect(
        host="192.168.43.86",  # your host, usually localhost
        user="root",  # your username
        password="said2002",  # password leave it blank if there isn't any
        port="3306",  # port (3306 default)
        db="ecommercedb", # name of the database
    )  

    pd.read_sql(sql=f"select * from `product`", con=conn).to_csv("/tmp/extract_product_data.csv", index=False)

def extract_order_data(**kawrgs):
    conn = Connect(
        host="192.168.43.86",  # your host, usually localhost
        user="root",  # your username
        password="said2002",  # password leave it blank if there isn't any
        port="3306",  # port (3306 default)
        db="ecommercedb", # name of the database
    )  

    pd.read_sql(sql=f"SELECT * FROM `order` o JOIN `order-item` oi ON o.order_id=oi.order_id ", con=conn).to_csv("/tmp/extract_order_data.csv", index=False)

def extract_date_data():

    # create a date range for the date dimension
    start_date = "2021-01-01"
    end_date = "2022-12-31"

    date_range = pd.date_range(start=start_date, end=end_date, freq="D")
    date_dimension = pd.DataFrame(date_range, columns=["date"])

    # add columns for year, quarter, month, day, and weekday
    date_dimension["day"] = date_dimension["date"].dt.day
    date_dimension["month"] = date_dimension["date"].dt.month
    date_dimension["quarter"] = date_dimension["date"].dt.quarter
    date_dimension["year"] = date_dimension["date"].dt.year
    date_dimension["month_name"] = date_dimension["date"].dt.strftime('%B')
    date_dimension['dayName'] =  date_dimension["date"].dt.strftime('%A')
    date_dimension["dayOfWeek"] = date_dimension["date"].dt.weekday
    date_dimension['dayOfMonth'] = date_dimension["date"].dt.strftime('%d') 

    date_dimension.to_csv("/tmp/date.csv", index=False)


'''
# 
# Transform Functions
# 
# 
''' 
def transform_customer_data():
    df = pd.read_csv("/tmp/extract_customer_data.csv")

    # drop null values
    df = df.dropna(axis="index", how="any")

    df["first_name"] = df["first_name"].str.strip().str.title()
    df["last_name"] = df["last_name"].str.strip().str.title()
    df["full_name"] = df["first_name"] + " " + df["last_name"]
    df["email"] = df["email"].str.strip().str.lower()

    # transform process ...

    # final columns
    cols = [
        "customer_id", "full_name", "phone_number", "email", "address"
    ]

    df = df[cols]

    # store data locally
    df.to_csv("/tmp/transformed_customer_data.csv", index=False)


def transform_product_data():
    
    df = pd.read_csv("/tmp/extract_product_data.csv")

    # drop null values
    df = df.dropna(axis="index", how="any")

    # transform process ...

    cols = [
        "product_id",
        "product_name",
        "product_brand",
        "product_category",
        "product_price",
    ]

    df = df[cols]

    df.to_csv("/tmp/transformed_product_data.csv", index=False)


def transform_order_data():
    
    df = pd.read_csv("/tmp/extract_order_data.csv")

    # drop null values
    df = df.dropna(axis="index", how="any")

    # transform process ...

    cols = [
        "order_id",
        "order_date",
        "customer_id",
        "product_id",
        "order_status", 
        "product_price",
        "quantity" 
    ]

    df = df[cols]

    df.to_csv("/tmp/transformed_order_data.csv", index=False)


'''
# 
# Load Functions 
# 
# 
''' 
def load_customer_data():
    
    try :
        conn = Connect(
            host="192.168.43.86",  # your host, usually localhost
            user="root",  # your username
            password="said2002",  # password leave it blank if there isn't any
            port="3306",  # port (3306 default)
            db="dw_example",
        )  # name of the database

        conn.autocommit = False

        cur = conn.cursor()
    except :
            raise Exception("MySql db connection error")

#  stagged data 
    df = pd.read_csv('/tmp/transformed_customer_data.csv')

    # sql statement
    sql_into_dim = f"""
        INSERT IGNORE INTO `customer_dim` (customer_id, full_name, phone_number, email, address)  VALUES(%s, %s, %s, %s, %s)
    """

    # insert rows to dimension table
    for i, row in df.iterrows():
        cur.execute(sql_into_dim, list(row))

    # commit changes
    conn.commit()
    
    conn.close()


def load_product_data():
    try :
        
        conn = Connect(
            host="192.168.43.86",  # your host, usually localhost
            user="root",  # your username
            password="said2002",  # password leave it blank if there isn't any
            port="3306",  # port (3306 default)
            db="dw_example",
        )  # name of the database

        conn.autocommit = False
        
        cur = conn.cursor()
    except :
            raise Exception("MySql db connection error")

    #  stagged data 
    df = pd.read_csv('/tmp/transformed_product_data.csv')

    # sql statement
    sql_into_dim = f"""
        INSERT IGNORE INTO `product_dim` (product_id, product_name, product_brand, product_category, product_price)  VALUES(%s, %s, %s, %s, %s);
    """

    # insert rows to dimension table
    for i, row in df.iterrows():
        cur.execute(sql_into_dim, list(row))

    # commit changes
    conn.commit()
    
    conn.close()
    

def load_order_data():
    try :
        
        conn = Connect(
            host="192.168.43.86",  # your host, usually localhost
            user="root",  # your username
            password="said2002",  # password leave it blank if there isn't any
            port="3306",  # port (3306 default)
            db="dw_example",
        )  # name of the database

        conn.autocommit = False

        cur = conn.cursor()
    except :
            raise Exception("MySql db connection error")

    #  stagged data 
    df = pd.read_csv('/tmp/transformed_order_data.csv')

    # sql statement
    sql_into_dim = f"""
        INSERT IGNORE INTO `order_dim` (
        order_id,
        order_date,
        customer_id,
        product_id,
        order_status, 
        product_price,
        quantity )  VALUES(%s, %s, %s, %s, %s, %s, %s);
    """

    # insert rows to dimension table
    for i, row in df.iterrows():
        cur.execute(sql_into_dim, list(row))

    # commit changes
    conn.commit()
    
    conn.close()
       
       
def load_date_data():
    try :
        
        conn = Connect(
            host="192.168.43.86",   # your host, usually localhost
            user="root",    # your username
            password="said2002",    # password leave it blank if there isn't any
            port="3306",    # port (3306 default)
            db="dw_example",    # name of the database
        )  

        conn.autocommit = False
        
        cur = conn.cursor()
    except :
            raise Exception("MySql db connection error")

    #  stagged data 
    df = pd.read_csv('/tmp/date.csv')

    # sql statement
    sql_into_dim = f"""
        INSERT IGNORE INTO `date_dim`  VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    # insert rows to dimension table
    for i, row in df.iterrows():
        cur.execute(sql_into_dim, list(row))

    # commit changes
    conn.commit()
    
    conn.close()
    
    
def create_fact_table():

    conn = Connect(
            host="192.168.43.86",  # your host, usually localhost
            user="root",  # your username
            password="said2002",  # password leave it blank if there isn't any
            port="3306",  # port (3306 default)
            db="dw_example",
        )  # name of the database
    
    cur = conn.cursor()
    
    sql = '''
        CREATE TABLE sales_fact (
            `date` DATE,
            `order_id` INT,
            `product_id` INT,
            `customer_id` INT,
            `total_amount` DECIMAL(19, 4),
            ) 
            SELECT o.order_date as 'date', 
                    o.order_id, p.product_id, 
                    c.customer_id, (o.quantity*o.product_price) as 'total_amount'
                        FROM order_dim as o
                        JOIN product_dim p ON p.product_id = o.product_id
                        JOIN customer_dim c ON c.customer_id = o.customer_id;
            
        ALTER TABLE sales_fact ADD FOREIGN KEY 
            (`date`) REFERENCES date_dim(`date`); 
            
        ALTER TABLE sales_fact ADD FOREIGN KEY 
            (`order_id`) FOREIGN KEY REFERENCES `order`(`order_id`);

        ALTER TABLE sales_fact ADD FOREIGN KEY 
            (`product_id`) FOREIGN KEY REFERENCES product_dim(`product_id`);

        ALTER TABLE sales_fact ADD FOREIGN KEY 
            (`customer_id`) FOREIGN KEY REFERENCES customer_dim(`customer_id`);
        ''' 
    cur.execute(sql)

    conn.commit()
    
    conn.close()


dag = DAG(
    "example_of_ecommerce_pipeline",
    start_date=datetime(2022, 3, 5),
    default_args=default_args,
    schedule_interval="@daily",
)

extract_customer_task = PythonOperator(
    task_id="extract_customer_data", 
    python_callable=extract_customer_data, 
    dag=dag
)

extract_product_task = PythonOperator(
    task_id="extract_product_data", 
    python_callable=extract_product_data, 
    dag=dag
)

extract_order_task = PythonOperator(
    task_id="extract_order_data", 
    python_callable=extract_order_data, 
    dag=dag
)

extract_date_task = PythonOperator(
    task_id="extract_date_data", 
    python_callable=extract_date_data, 
    dag=dag
)

transform_customer_task = PythonOperator(
    task_id="transform_customer_data", 
    python_callable=transform_customer_data, 
    dag=dag
)

transform_product_task = PythonOperator(
    task_id="transform_product_data", 
    python_callable=transform_product_data, 
    dag=dag
)

transform_order_task = PythonOperator(
    task_id="transform_order_data", 
    python_callable=transform_order_data, 
    dag=dag
)

load_data_task = PythonOperator(
    task_id="load_dim_dimension", 
    python_callable=load_date_data, 
    dag=dag
)

# load customer dimension data task 
load_customer_dim_task = PythonOperator(
    task_id="load_customer_dim_data",
    python_callable=load_customer_data,
    dag=dag,
)

# load product dimension data task 
load_product_dim_task = PythonOperator(
    task_id="load_product_dim_data",
    python_callable=load_product_data,
    dag=dag,
)

# load order dimension data task 
load_order_dim_task = PythonOperator(
    task_id="load_order_dim_data",
    python_callable=load_order_data,
    dag=dag,
)


# create fact sales table task 
create_fact_sales = PythonOperator(
    task_id="create_fact_table",
    python_callable=create_fact_table,
    dag=dag,
)

[extract_customer_task >> transform_customer_task >> load_customer_dim_task,
extract_product_task >> transform_product_task >> load_product_dim_task,
extract_order_task >> transform_order_task >> load_order_dim_task,
extract_date_task >> load_data_task] >> create_fact_sales 