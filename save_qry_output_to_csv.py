from pyhive import hive
import pandas as pd

# Define the Hive connection parameters
hive_host = "localhost"
hive_port = 10000
hive_username = "your_hive_username"

# Hive query to calculate the average discount for each category
average_discount_query = """
SELECT
    category,
    AVG(discount_no_null) AS avg_discount
FROM __order_detail_new__
GROUP BY category
"""

# Hive query to calculate row count per each cooking_bin
row_count_query = """
SELECT
    cooking_bin,
    COUNT(*) AS row_count
FROM __restaurant_detail_new__
GROUP BY cooking_bin
"""

# Path to the output directory where CSV files will be saved
output_directory = '/path/to/output/directory/'

try:
    # Establish a connection to Hive
    connection = hive.Connection(host=hive_host, port=hive_port, username=hive_username)

    # Execute the average discount query
    with connection.cursor() as cursor:
        cursor.execute(average_discount_query)
        result = cursor.fetchall()

    # Create a DataFrame from the query result
    df_avg_discount = pd.DataFrame(result, columns=['category', 'avg_discount'])

    # Save the result to discount.csv
    df_avg_discount.to_csv(output_directory + 'discount.csv', index=False)

    # Execute the row count query
    with connection.cursor() as cursor:
        cursor.execute(row_count_query)
        result = cursor.fetchall()

    # Create a DataFrame from the query result
    df_row_count = pd.DataFrame(result, columns=['cooking_bin', 'row_count'])

    # Save the result to cooking.csv
    df_row_count.to_csv(output_directory + 'cooking.csv', index=False)

    print("Query results saved to CSV files 'discount.csv' and 'cooking.csv' successfully.")

except Exception as e:
    print(f"Error: {str(e)}")
finally:
    # Close the Hive connection
    connection.close()
