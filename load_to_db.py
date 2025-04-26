#!/usr/bin/env python3
import pandas as pd
import mysql.connector
from mysql.connector import Error
import os
from tqdm import tqdm

# Database configuration
DB_CONFIG = {
    'host': 'sql.freedb.tech',
    'port': 3306,
    'database': 'freedb_dbacademyawards',
    'user': 'freedb_yomna',
    'password': 'x5V7z?FZUQqVwUR'
}

def create_connection():
    """Create a connection to the MySQL database"""
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        if connection.is_connected():
            print(f"Successfully connected to {DB_CONFIG['database']}")
            return connection
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None

def load_csv_to_table(connection, csv_file, table_name):
    """Load data from a CSV file into a MySQL table"""
    try:
        # Read CSV file
        print(f"\nLoading {csv_file} into {table_name}...")
        df = pd.read_csv(f'data/{csv_file}')
        cursor = connection.cursor()
        
        # Get column names
        columns = df.columns.tolist()
        columns_str = ', '.join(columns)
        placeholders = ', '.join(['%s'] * len(columns))
        
        # Prepare insert query
        insert_query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
        
        # Insert data row by row with progress bar
        for _, row in tqdm(df.iterrows(), total=len(df), desc=f"Inserting into {table_name}"):
            values = tuple(None if pd.isna(val) else val for val in row)
            try:
                cursor.execute(insert_query, values)
            except Error as e:
                print(f"\nError inserting row into {table_name}: {e}")
                print(f"Row values: {values}")
                continue
        
        # Commit the changes
        connection.commit()
        print(f"Successfully loaded {len(df)} rows into {table_name}")
        
    except Error as e:
        print(f"Error loading {csv_file} into {table_name}: {e}")
    except Exception as e:
        print(f"Unexpected error loading {csv_file}: {e}")

def main():
    # Create database connection
    connection = create_connection()
    if not connection:
        return
    
    try:
        # Define the order of tables to load (based on foreign key dependencies)
        tables_to_load = [
            ('persons_fixed.csv', 'persons'),  # Use persons_fixed.csv for persons table
            ('movies.csv', 'movies'),
            ('categories.csv', 'categories'),
            ('venues.csv', 'venues'),
            ('positions.csv', 'positions'),
            ('award_editions.csv', 'award_editions'),
            ('nominations.csv', 'nominations'),
            ('movie_crew.csv', 'movie_crew'),
            ('nomination_person.csv', 'nomination_person'),
            ('award_edition_person.csv', 'award_edition_person')
        ]
        
        # Load each table
        for csv_file, table_name in tables_to_load:
            if os.path.exists(f'data/{csv_file}'):
                load_csv_to_table(connection, csv_file, table_name)
            else:
                print(f"\nWarning: {csv_file} not found in data directory")
        
    except Error as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
    finally:
        if connection.is_connected():
            connection.close()
            print("\nDatabase connection closed.")

if __name__ == "__main__":
    main() 