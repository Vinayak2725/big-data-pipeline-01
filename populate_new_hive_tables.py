from pyhive import hive

# Define the Hive connection parameters
hive_host = "localhost"
hive_port = 10000
hive_username = "your_hive_username"

# Hive query to populate the __order_detail_new__ table
populate___order_detail_new___table = """
INSERT OVERWRITE TABLE __order_detail_new__ PARTITION (dt)
SELECT
    order_created_timestamp,
    status,
    price,
    COALESCE(discount, 0) AS discount_no_null,
    id,
    driver_id,
    user_id,
    restaurant_id,
    dt
FROM order_detail
"""

# Hive query to populate the __restaurant_detail_new__ table
populate___restaurant_detail_new___table = """
INSERT OVERWRITE TABLE ____restaurant_detail_new____ PARTITION (dt)
SELECT
    id,
    restaurant_name,
    category,
    estimated_cooking_time,
    latitude,
    longitude,
    dt,
    CASE
        WHEN estimated_cooking_time BETWEEN 10 AND 40 THEN 1
        WHEN estimated_cooking_time BETWEEN 41 AND 80 THEN 2
        WHEN estimated_cooking_time BETWEEN 81 AND 120 THEN 3
        ELSE 4
    END AS cooking_bin
FROM restaurant_detail
"""

try:
    # Establish a connection to Hive
    connection = hive.Connection(host=hive_host, port=hive_port, username=hive_username)

    # Populate the __order_detail_new__ table in Hive
    with connection.cursor() as cursor:
        cursor.execute(populate___order_detail_new___table)

    # Populate the __restaurant_detail_new__ table in Hive
    with connection.cursor() as cursor:
        cursor.execute(populate___restaurant_detail_new___table)

    print("Data populated into Hive tables '__order_detail_new__' and '__restaurant_detail_new__' successfully.")

except Exception as e:
    print(f"Error: {str(e)}")
finally:
    # Close the Hive connection
    connection.close()
