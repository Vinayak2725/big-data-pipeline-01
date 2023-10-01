from pyhive import hive

# Define the Hive connection parameters
hive_host = "localhost"
hive_port = 10000
hive_username = "your_hive_username"

# Hive query to create the order_detail table
create_order_detail_table = """
CREATE EXTERNAL TABLE IF NOT EXISTS order_detail (
    order_created_timestamp STRING,
    status STRING,
    price INT,
    discount FLOAT,
    id STRING,
    driver_id STRING,
    user_id STRING,
    restaurant_id STRING
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION '/user/hive/warehouse/order_detail'
TBLPROPERTIES ('parquet.compression'='SNAPPY')
"""

# Hive query to create the restaurant_detail table
create_restaurant_detail_table = """
CREATE EXTERNAL TABLE IF NOT EXISTS restaurant_detail (
    id STRING,
    restaurant_name STRING,
    category STRING,
    estimated_cooking_time FLOAT,
    latitude FLOAT,
    longitude FLOAT
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION '/user/hive/warehouse/restaurant_detail'
TBLPROPERTIES ('parquet.compression'='SNAPPY')
"""

try:
    # Establish a connection to Hive
    connection = hive.Connection(host=hive_host, port=hive_port, username=hive_username)

    # Create the order_detail table in Hive
    with connection.cursor() as cursor:
        cursor.execute(create_order_detail_table)

    # Create the restaurant_detail table in Hive
    with connection.cursor() as cursor:
        cursor.execute(create_restaurant_detail_table)

    print("Hive tables created successfully.")

except Exception as e:
    print(f"Error: {str(e)}")
finally:
    # Close the Hive connection
    connection.close()
