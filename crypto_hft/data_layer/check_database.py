from crypto_hft.utils.config import Config
import mysql.connector

# Load configuration
config = Config()

# MySQL connection configuration
DB_CONFIG = {
    'database': config.db_database,
    'user': config.db_user,
    'password': config.db_password,
    'host': config.db_host,
    'port': 3306,  
    'charset': 'utf8',  
}

try:
    # Connect to MySQL using mysql-connector
    conn = mysql.connector.connect(**DB_CONFIG)
    print("✅ Connected to MySQL successfully!")

    cursor = conn.cursor()

    # Get the list of tables in the database
    cursor.execute("SHOW TABLES;")
    tables = cursor.fetchall()

    if tables:
        print("✅ Tables in the database:")
        for table in tables:
            print(table[0])  # Print table name

            # Fetch and print the schema (columns) for each table
            print(f"Schema for {table[0]}:")
            cursor.execute(f"DESCRIBE {table[0]};")  # Describe the table to get its schema
            columns = cursor.fetchall()

            for column in columns:
                print(f"  Column: {column[0]}, Type: {column[1]}, Nullable: {column[2]}, Default: {column[4]}")
            print("-" * 50)  # Separator for better readability

    else:
        print("❌ No tables found in the database!")

    # Close the connection
    conn.close()

except mysql.connector.Error as e:
    print(f"❌ Connection error: {e}")

# from crypto_hft.utils.config import Config
# import mysql.connector

# # Load configuration
# config = Config()

# # MySQL connection configuration
# DB_CONFIG = {
#     'database': config.db_database,
#     'user': config.db_user,
#     'password': config.db_password,
#     'host': config.db_host,
#     'port': 3306,  
#     'charset': 'utf8',  
# }

# try:
#     # Connect to MySQL using mysql-connector
#     conn = mysql.connector.connect(**DB_CONFIG)
#     print("✅ Connected to MySQL successfully!")

#     cursor = conn.cursor()

#     # Get the list of tables in the database
#     cursor.execute("SHOW TABLES;")
#     tables = cursor.fetchall()

#     if tables:
#         print("✅ Tables in the database:")
#         for table in tables:
#             table_name = table[0]
#             print(table_name)  # Print table name

#             # Fetch and print the schema (columns) for each table
#             print(f"Schema for {table_name}:")
#             cursor.execute(f"DESCRIBE {table_name};")  # Describe the table to get its schema
#             columns = cursor.fetchall()

#             for column in columns:
#                 print(f"  Column: {column[0]}, Type: {column[1]}, Nullable: {column[2]}, Default: {column[4]}")
#             print("-" * 50)  # Separator for better readability

#             # Delete the table
#             try:
#                 cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
#                 print(f"✅ Table {table_name} deleted successfully!")
#             except mysql.connector.Error as e:
#                 print(f"❌ Failed to delete table {table_name}: {e}")

#     else:
#         print("❌ No tables found in the database!")

#     # Commit changes (if any)
#     conn.commit()

#     # Close the connection
#     conn.close()

# except mysql.connector.Error as e:
#     print(f"❌ Connection error: {e}")


