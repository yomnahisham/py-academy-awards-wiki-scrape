#!/usr/bin/env python3
import pandas as pd
import os
import sys
import time
from tqdm import tqdm
import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime

# Import functions from fix_persons_csv.py
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from fix_persons_csv import (
    clean_text, 
    is_invalid_name, 
    get_soup, 
    normalize_date, 
    build_wiki_url, 
    extract_person_details, 
    normalize_country, 
    process_person
)

def main():
    print("Starting to process remaining rows...")
    
    # Load the original persons.csv file
    persons_df = pd.read_csv('data/persons.csv')
    print(f"Loaded {len(persons_df)} rows from persons.csv")
    
    # Load the existing persons_fixed.csv file
    fixed_df = pd.read_csv('data/persons_fixed.csv')
    print(f"Loaded {len(fixed_df)} rows from persons_fixed.csv")
    
    # Load the relations files
    movie_crew_df = pd.read_csv('data/movie_crew.csv')
    nomination_person_df = pd.read_csv('data/nomination_person.csv')
    
    # Get person IDs that have connections
    connected_ids = set(movie_crew_df['person_id'].astype(str)).union(
        set(nomination_person_df['person_id'].astype(str))
    )
    print(f"Found {len(connected_ids)} persons with connections")
    
    # Get the IDs that have already been processed
    processed_ids = set(fixed_df['person_id'].astype(str))
    print(f"Found {len(processed_ids)} already processed IDs")
    
    # Filter out rows that have already been processed and only keep those with connections
    remaining_df = persons_df[
        (~persons_df['person_id'].astype(str).isin(processed_ids)) &
        (persons_df['person_id'].astype(str).isin(connected_ids))
    ]
    print(f"Found {len(remaining_df)} remaining rows to process")
    
    if len(remaining_df) == 0:
        print("No remaining rows to process. Exiting.")
        return
    
    # Process the remaining rows
    results = []
    total_rows = len(remaining_df)
    for idx, row in tqdm(remaining_df.iterrows(), total=total_rows, desc="Processing remaining rows"):
        try:
            # Process the person with the required tuple format
            result = process_person((idx, row, total_rows))
            if result is not None:
                results.append(result)
        except Exception as e:
            print(f"Error processing person {row['person_id']}: {e}")
    
    # Convert results to DataFrame
    if results:
        new_rows_df = pd.DataFrame(results)
        print(f"Processed {len(new_rows_df)} rows successfully")
        
        # Append to the existing file
        new_rows_df.to_csv('data/persons_fixed.csv', mode='a', header=False, index=False)
        print(f"Appended {len(new_rows_df)} rows to persons_fixed.csv")
    else:
        print("No rows were processed successfully")

if __name__ == "__main__":
    start_time = time.time()
    main()
    end_time = time.time()
    print(f"Total execution time: {end_time - start_time:.2f} seconds") 