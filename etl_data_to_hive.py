#etl_data_to_hive.py

import subprocess

def etl_to_hive():
    try:
        # Sqoop command for order_detail table
        sqoop_order_detail = """
        sqoop import \
            --connect jdbc:postgresql://localhost:5432/yourdb \
            --username yourusername \
            --password yourpassword \
            --table order_detail \
            --hive-import \
            --hive-table order_detail \
            --hive-partition-key dt \
            --hive-partition-value latest \
            --create-hive-table \
            --as-parquetfile
        """
        
        # Sqoop command for restaurant_detail table
        sqoop_restaurant_detail = """
        sqoop import \
            --connect jdbc:postgresql://localhost:5432/yourdb \
            --username yourusername \
            --password yourpassword \
            --table restaurant_detail \
            --hive-import \
            --hive-table restaurant_detail \
            --hive-partition-key dt \
            --hive-partition-value latest \
            --create-hive-table \
            --as-parquetfile
        """

        # Execute the Sqoop commands
        subprocess.run(sqoop_order_detail, shell=True, check=True)
        subprocess.run(sqoop_restaurant_detail, shell=True, check=True)

        print("Data extracted from PostgreSQL and loaded into Hive successfully.")

    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    etl_to_hive()
