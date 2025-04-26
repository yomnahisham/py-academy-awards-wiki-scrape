#!/usr/bin/env python3
"""
Import CSV data to MySQL/MariaDB database
This script helps import the Academy Awards data from CSV files into a MySQL database.
Uses LOAD DATA INFILE for efficient bulk loading to minimize query count.
"""

import os
import sys
import pandas as pd
import mysql.connector
from mysql.connector import Error
from tqdm import tqdm
from datetime import datetime
import numpy as np
import tempfile
import time

# Database configuration
DB_CONFIG = {
    'host': 'database-mysql.cre8ag8cwsfq.eu-north-1.rds.amazonaws.com',
    'port': 3306,
    'database': 'academy_awards',
    'user': 'admin',
    'password': 'Ohtobeloved9'
}

def create_database_if_not_exists():
    """Create the database if it doesn't exist"""
    try:
        # Connect without specifying database
        config = DB_CONFIG.copy()
        config.pop('database', None)
        connection = mysql.connector.connect(**config)
        cursor = connection.cursor()
        
        # Create database if it doesn't exist
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_CONFIG['database']}")
        print(f"Database {DB_CONFIG['database']} created or already exists")
        
        cursor.close()
        connection.close()
        return True
    except Error as e:
        print(f"Error creating database: {e}")
        return False

def connect_to_database():
    try:
        # First ensure database exists
        if not create_database_if_not_exists():
            return None
            
        connection = mysql.connector.connect(**DB_CONFIG)
        return connection
    except Error as e:
        print(f"Error connecting to database: {e}")
        print("Please check if:")
        print("1. The database server is running")
        print("2. The credentials are correct")
        print("3. Your IP is whitelisted")
        return None

def clean_dataframe(df, table_name):
    """Clean the dataframe before importing"""
    # Replace NaN with None for proper SQL NULL handling
    df = df.replace({np.nan: None})
    
    # Table-specific cleaning
    if table_name == 'venue':
        # Truncate venue_name to 60 characters
        if 'venue_name' in df.columns:
            df['venue_name'] = df['venue_name'].str[:60]
            print("Truncated venue names to 60 characters")
    elif table_name == 'movie':
        # Handle duplicate run_time columns
        if 'run_time.1' in df.columns:
            # Use the first non-null value between run_time and run_time.1
            df['run_time'] = df.apply(lambda row: row['run_time'] if pd.notnull(row['run_time']) else row['run_time.1'], axis=1)
            df = df.drop('run_time.1', axis=1)
            print("Fixed duplicate run_time columns")
    elif table_name == 'movie_produced_by':
        # Rename production_company_id to pd_id if needed
        if 'production_company_id' in df.columns:
            df = df.rename(columns={'production_company_id': 'pd_id'})
    
    return df

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

def wait_and_retry(func, max_retries=3, delay=5):
    for attempt in range(max_retries):
        try:
            return func()
        except Error as e:
            if "max_questions" in str(e) and attempt < max_retries - 1:
                print(f"Rate limit hit, waiting {delay} seconds...")
                time.sleep(delay)
                delay *= 2  # Exponential backoff
                continue
            raise e

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

def convert_value(val):
    """Convert values to proper SQL types"""
    if pd.isna(val):
        return None
    if pd.api.types.is_integer_dtype(type(val)):
        return int(val)
    if pd.api.types.is_float_dtype(type(val)):
        return float(val)
    if isinstance(val, str):
        return val.strip()
    return val

def import_table(connection, table_name, csv_file, batch_size=1000):
    try:
        cursor = connection.cursor()
        
        # Clear the table before importing
        clear_table(connection, table_name)
        
        # Read CSV in chunks
        chunk_size = batch_size
        for chunk in pd.read_csv(csv_file, chunksize=chunk_size):
            # Clean the data
            chunk = clean_dataframe(chunk, table_name)
            
            # Replace NaN with None
            chunk = chunk.replace({np.nan: None})
            chunk = chunk.where(pd.notnull(chunk), None)
            
            # Prepare insert query
            placeholders = ', '.join(['%s'] * len(chunk.columns))
            columns = ', '.join(f"`{col}`" for col in chunk.columns)  # Escape column names
            insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            
            # Convert DataFrame to list of tuples, properly handling all data types
            values = [tuple(convert_value(x) for x in row) for row in chunk.to_numpy()]
            
            # Execute in larger batches
            try:
                cursor.executemany(insert_query, values)
                connection.commit()
                time.sleep(2)  # Reduced delay between batches
            except Error as e:
                if "Duplicate entry" in str(e):
                    print(f"Warning: Duplicate entries found in {table_name}, skipping...")
                    continue
                print(f"Error in batch insert: {e}")
                print(f"Problematic query: {insert_query}")
                print(f"First row of problematic batch: {values[0] if values else None}")
                raise e
            
    except Error as e:
        print(f"Error importing {table_name}: {e}")
        raise e

def test_connection():
    """Test database connection and print connection info"""
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        if connection.is_connected():
            db_info = connection.get_server_info()
            cursor = connection.cursor()
            cursor.execute("SELECT DATABASE();")
            db_name = cursor.fetchone()[0]
            print(f"Connected to MySQL Server version {db_info}")
            print(f"Connected to database: {db_name}")
            
            # Test a simple query
            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()
            print("\nAvailable tables:")
            for table in tables:
                print(f"- {table[0]}")
            
            connection.close()
            return True
    except Error as e:
        print(f"Error connecting to database: {e}")
        print("Please check if:")
        print("1. The database server is running")
        print("2. The credentials are correct")
        print("3. Your IP is whitelisted")
        return False

def check_table_structure(connection, table_name):
    """Check the structure of a table"""
    try:
        cursor = connection.cursor()
        cursor.execute(f"DESCRIBE {table_name}")
        columns = cursor.fetchall()
        print(f"\nTable structure for {table_name}:")
        for col in columns:
            print(f"- {col[0]}: {col[1]}")
    except Error as e:
        print(f"Error checking table structure: {e}")

def verify_table_count(connection, table_name):
    """Verify the number of records in a table"""
    try:
        cursor = connection.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        print(f"Table {table_name} has {count} records")
        return count
    except Error as e:
        print(f"Error checking count for {table_name}: {e}")
        return 0

def main():
    # Test connection first
    if not test_connection():
        return
        
    connection = connect_to_database()
    if not connection:
        return
    
    try:
        # Check table structure
        check_table_structure(connection, 'venue')
        
        # Disable foreign key checks before import
        disable_foreign_key_checks(connection)
        
        # Import tables in order of dependencies
        tables = [
            ('data/venues.csv', 'venue'),
            ('data/positions.csv', 'positions'),
            ('data/persons_fixed_clean.csv', 'person'),  # Fixed file name
            ('data/movies.csv', 'movie'),
            ('data/award_editions.csv', 'award_edition'),
            ('data/categories.csv', 'category'),
            ('data/nominations.csv', 'nomination'),
            ('data/movie_crew.csv', 'movie_crew'),
            ('data/movie_language.csv', 'movie_language'),
            ('data/movie_country.csv', 'movie_country'),
            ('data/movie_release_date.csv', 'movie_release_date'),
            ('data/production_company.csv', 'production_company'),
            ('data/movie_produced_by.csv', 'movie_produced_by'),
            ('data/nomination_person.csv', 'nomination_person'),
            ('data/award_edition_person.csv', 'award_edition_person')
        ]
        
        for csv_file, table_name in tables:
            print(f"\nImporting {csv_file}...")
            wait_and_retry(lambda: import_table(connection, table_name, csv_file))
            verify_table_count(connection, table_name)
        
        # Re-enable foreign key checks after import
        enable_foreign_key_checks(connection)
        print("\nImport process completed. Checking for any foreign key violations...")
        
    finally:
        connection.close()

if __name__ == "__main__":
    main() 