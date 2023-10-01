#create_postgresql_tables.py


import psycopg2

# PostgreSQL connection parameters
db_params = {
    "dbname": "yourdb",
    "user": "yourusername",
    "password": "yourpassword",
    "host": "localhost",
    "port": "5432",
}

# SQL statement to create tables
create_tables_sql = """
CREATE TABLE IF NOT EXISTS order_detail (
    order_created_timestamp TIMESTAMP,
    status VARCHAR(255),
    price INTEGER,
    discount FLOAT,
    id VARCHAR(255),
    driver_id VARCHAR(255),
    user_id VARCHAR(255),
    restaurant_id VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS restaurant_detail (
    id VARCHAR(255),
    restaurant_name VARCHAR(255),
    category VARCHAR(255),
    estimated_cooking_time REAL,
    latitude REAL,
    longitude REAL
);
"""

def create_postgresql_tables():
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()

        # Create tables
        cursor.execute(create_tables_sql)
        conn.commit()

        # Close the cursor and connection
        cursor.close()
        conn.close()

        print("PostgreSQL tables created successfully.")

    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    create_postgresql_tables()
