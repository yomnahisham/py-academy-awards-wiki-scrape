#!/usr/bin/env python3
"""
Import only production company data to MySQL database
"""

import pandas as pd
import mysql.connector
from mysql.connector import Error
import numpy as np

# Database configuration
DB_CONFIG = {
    'host': 'database-mysql.cre8ag8cwsfq.eu-north-1.rds.amazonaws.com',
    'port': 3306,
    'database': 'academy_awards',
    'user': 'admin',
    'password': 'Ohtobeloved9'
}

def connect_to_database():
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        return connection
    except Error as e:
        print(f"Error connecting to database: {e}")
        return None

def clear_table(connection, table_name):
    """Clear all data from a table"""
    try:
        cursor = connection.cursor()
        cursor.execute(f"TRUNCATE TABLE {table_name}")
        connection.commit()
        print(f"Cleared table {table_name}")
    except Error as e:
        print(f"Error clearing table {table_name}: {e}")
        raise e

def disable_foreign_key_checks(connection):
    cursor = connection.cursor()
    cursor.execute("SET FOREIGN_KEY_CHECKS=0")
    connection.commit()
    print("Disabled foreign key checks")

def enable_foreign_key_checks(connection):
    cursor = connection.cursor()
    cursor.execute("SET FOREIGN_KEY_CHECKS=1")
    connection.commit()
    print("Re-enabled foreign key checks")

def import_production_companies(connection, csv_file):
    try:
        cursor = connection.cursor()
        
        # Disable foreign key checks
        disable_foreign_key_checks(connection)
        
        # Clear the table before importing
        clear_table(connection, 'production_company')
        
        # Read CSV file
        df = pd.read_csv(csv_file)
        
        # Replace NaN with None
        df = df.replace({np.nan: None})
        
        # Prepare insert query with IGNORE
        placeholders = ', '.join(['%s'] * len(df.columns))
        columns = ', '.join(f"`{col}`" for col in df.columns)
        insert_query = f"INSERT IGNORE INTO production_company ({columns}) VALUES ({placeholders})"
        
        # Convert DataFrame to list of tuples
        values = [tuple(None if pd.isna(x) else x for x in row) for row in df.to_numpy()]
        
        # Execute insert
        cursor.executemany(insert_query, values)
        connection.commit()
        
        # Re-enable foreign key checks
        enable_foreign_key_checks(connection)
        
        # Verify count
        cursor.execute("SELECT COUNT(*) FROM production_company")
        count = cursor.fetchone()[0]
        print(f"Successfully imported {count} production companies")
        
    except Error as e:
        print(f"Error importing production companies: {e}")
        raise e

def main():
    connection = connect_to_database()
    if not connection:
        return
    
    try:
        import_production_companies(connection, 'data/production_company.csv')
    finally:
        connection.close()

if __name__ == "__main__":
    main() 