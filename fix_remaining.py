#!/usr/bin/env python3
"""
Script to process all records from the persons.csv file with optimized settings for speed
Only processes persons that have relevant connections to movies or nominations
"""

import argparse
import time
import logging
import os
import pandas as pd
import gc
import sys

# Import all functions from fix_persons_csv
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from fix_persons_csv import (
    process_person, update_stats, clean_text, is_invalid_name, 
    remove_invalid_persons, get_soup, normalize_date, build_wiki_url, 
    extract_person_details, normalize_country, process_chunk
)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)

# Constants - optimized for speed and efficiency
INPUT_CSV = 'data/persons.csv'
OUTPUT_CSV = 'data/persons_fixed.csv'
MAX_WORKERS = 50  # Optimized from 100 to 50 for more efficient processing
CHUNK_SIZE = 1000  # Large chunk size for efficient processing
SAVE_INTERVAL = 1000  # Save after every 1000 records

# Files with person relations
MOVIE_CREW_CSV = 'data/movie_crew.csv'
NOMINATION_PERSON_CSV = 'data/nomination_person.csv'

# Access the stats dictionary from fix_persons_csv
from fix_persons_csv import stats

def check_person_relations(person_id):
    """Check if a person has any relations in movie_crew or nomination_person tables"""
    # Initialize result to False
    has_relations = False
    
    # Check movie_crew table if it exists
    if os.path.exists(MOVIE_CREW_CSV):
        try:
            with open(MOVIE_CREW_CSV, 'r') as f:
                for line in f:
                    if f",{person_id}," in line or line.startswith(f"{person_id},"):
                        has_relations = True
                        break
        except Exception as e:
            logging.warning(f"Error checking movie_crew.csv: {e}")
    
    # If already found relation, no need to check further
    if has_relations:
        return True
        
    # Check nomination_person table if it exists
    if os.path.exists(NOMINATION_PERSON_CSV):
        try:
            with open(NOMINATION_PERSON_CSV, 'r') as f:
                for line in f:
                    if f",{person_id}," in line or line.startswith(f"{person_id},"):
                        has_relations = True
                        break
        except Exception as e:
            logging.warning(f"Error checking nomination_person.csv: {e}")
    
    return has_relations

def get_persons_with_relations(df):
    """Filter the persons dataframe to only include those with relations"""
    logging.info("Checking which persons have relationships in other tables...")
    start_time = time.time()
    
    # Create a set of person IDs with relations for faster lookup
    persons_with_relations = set()
    
    # Process movie_crew.csv
    if os.path.exists(MOVIE_CREW_CSV):
        try:
            movie_crew_df = pd.read_csv(MOVIE_CREW_CSV)
            if 'person_id' in movie_crew_df.columns:
                # Convert to strings to ensure consistent comparison
                persons_with_relations.update(movie_crew_df['person_id'].astype(str).unique())
                logging.info(f"Found {len(persons_with_relations)} persons with movie crew relations")
        except Exception as e:
            logging.warning(f"Error reading movie_crew.csv: {e}")
    
    # Process nomination_person.csv
    if os.path.exists(NOMINATION_PERSON_CSV):
        try:
            nomination_person_df = pd.read_csv(NOMINATION_PERSON_CSV)
            if 'person_id' in nomination_person_df.columns:
                # Convert to strings to ensure consistent comparison
                persons_with_relations.update(nomination_person_df['person_id'].astype(str).unique())
                logging.info(f"Found {len(persons_with_relations)} total persons with relations")
        except Exception as e:
            logging.warning(f"Error reading nomination_person.csv: {e}")
    
    # Debug information about the first few relation IDs
    if persons_with_relations:
        sample_ids = list(persons_with_relations)[:5]
        logging.info(f"Sample relation IDs (first 5): {sample_ids}")
        
        # Debug information about the first few person IDs in the dataframe
        if 'person_id' in df.columns and not df.empty:
            sample_df_ids = df['person_id'].astype(str).iloc[:5].tolist()
            logging.info(f"Sample person IDs in dataframe (first 5): {sample_df_ids}")
    
    # Filter the dataframe to only include persons with relations
    if 'person_id' in df.columns and persons_with_relations:
        # Convert both to strings for proper comparison
        filtered_df = df[df['person_id'].astype(str).isin(persons_with_relations)].copy()
        skipped_count = len(df) - len(filtered_df)
        
        logging.info(f"Filtered out {skipped_count} persons without any relations")
        logging.info(f"Keeping {len(filtered_df)} persons with relations for processing")
        
        # As a fallback, if filtering resulted in empty dataframe, use original
        if len(filtered_df) == 0:
            logging.warning("Filtering resulted in EMPTY dataframe! Using original dataframe instead.")
            filtered_df = df.copy()
        
        elapsed_time = time.time() - start_time
        logging.info(f"Relation checking completed in {elapsed_time:.2f} seconds")
        
        return filtered_df
    else:
        logging.warning("No person_id column found in dataframe or no relations found. Processing all records.")
        return df

def fix_all_records(start_from=0, limit=None):
    """
    Process all records from persons.csv with optimized settings
    Directly writes to output file instead of using interim files
    Only processes persons with relations to movies or nominations
    """
    logging.info(f"Reading input CSV: {INPUT_CSV}")
    
    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
    
    try:
        # First get the cleaned dataframe without invalid records
        df = remove_invalid_persons()
        
        # Filter out persons without any relations
        df = get_persons_with_relations(df)
        
        total_rows = len(df)
        logging.info(f"Total valid rows with relations: {total_rows}")
        
        # Start from the specified index
        if start_from > 0 and start_from < total_rows:
            logging.info(f"Starting from record {start_from}")
            df = df.iloc[start_from:].copy()
            processed_so_far = start_from
        else:
            processed_so_far = 0
            
        remaining_rows = len(df)
        logging.info(f"Processing {remaining_rows} rows")
        
        # Limit the number of rows if requested
        if limit and limit > 0 and limit < remaining_rows:
            logging.info(f"Limiting to {limit} rows for testing")
            df = df.iloc[:limit].copy()
            remaining_rows = len(df)
        
        # Columns to keep in the fixed output
        output_columns = ['person_id', 'first_name', 'middle_name', 'last_name', 'birthDate', 'country', 'deathDate']
        
        # Make sure all required columns exist
        for col in output_columns:
            if col not in df.columns:
                if col == 'person_id':
                    df[col] = None
                else:
                    df[col] = pd.Series(dtype='object')
            elif col != 'person_id':
                df[col] = df[col].astype('object')
        
        # Process data in chunks to reduce memory usage
        all_updated_rows = []
        first_save = True
        
        # Reset stats for a clean count
        stats['updated_countries'] = 0
        stats['updated_birthdates'] = 0
        stats['updated_deathdates'] = 0
        stats['skipped_persons'] = 0
        
        # Split data into chunks
        for chunk_start in range(0, remaining_rows, CHUNK_SIZE):
            chunk_end = min(chunk_start + CHUNK_SIZE, remaining_rows)
            chunk_df = df.iloc[chunk_start:chunk_end].copy()
            
            # Adjust the indices for logging to show absolute position in original dataset
            absolute_start = processed_so_far + chunk_start
            absolute_end = processed_so_far + chunk_end
            logging.info(f"Processing chunk {absolute_start}-{absolute_end} of {total_rows}")
            
            # Process the chunk - use absolute indices for progress reporting
            updated_rows = process_chunk(chunk_df, absolute_start, total_rows)
            all_updated_rows.extend(updated_rows)
            
            # Save results directly to output file periodically
            if len(all_updated_rows) >= SAVE_INTERVAL:
                temp_df = pd.DataFrame(all_updated_rows)
                if not temp_df.empty:
                    temp_df = temp_df[output_columns].copy()
                    
                    # For first save, create new file
                    if first_save:
                        temp_df.to_csv(OUTPUT_CSV, index=False)
                        logging.info(f"Created output file with {len(all_updated_rows)} rows")
                        first_save = False
                    else:
                        # For subsequent saves, append to existing file without header
                        temp_df.to_csv(OUTPUT_CSV, mode='a', header=False, index=False)
                        logging.info(f"Appended {len(all_updated_rows)} more rows to output file")
                
                # Free memory
                all_updated_rows = []
                gc.collect()
        
        # Save any remaining processed rows
        if all_updated_rows:
            temp_df = pd.DataFrame(all_updated_rows)
            if not temp_df.empty:
                temp_df = temp_df[output_columns].copy()
                
                # For first save, create new file
                if first_save:
                    temp_df.to_csv(OUTPUT_CSV, index=False)
                    logging.info(f"Created output file with {len(all_updated_rows)} rows")
                else:
                    # For subsequent saves, append to existing file without header
                    temp_df.to_csv(OUTPUT_CSV, mode='a', header=False, index=False)
                    logging.info(f"Appended final {len(all_updated_rows)} rows to output file")
        
        # Print final stats    
        with open(OUTPUT_CSV, 'r') as f:
            final_line_count = sum(1 for _ in f) - 1  # Subtract header
            
        logging.info(f"\nSummary of fixes:")
        logging.info(f"- Updated countries: {stats['updated_countries']}")
        logging.info(f"- Updated birth dates: {stats['updated_birthdates']}")
        logging.info(f"- Updated death dates: {stats['updated_deathdates']}")
        logging.info(f"- Skipped persons: {stats['skipped_persons']}")
        logging.info(f"- Total processed records: {final_line_count}")
        logging.info(f"Fixed data saved to: {OUTPUT_CSV}")
        
    except Exception as e:
        logging.error(f"Error processing CSV: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    return True

if __name__ == "__main__":
    logging.info("Starting processing with optimized speed settings...")
    
    parser = argparse.ArgumentParser(description='Process persons from persons.csv with relations to movies/nominations')
    parser.add_argument('--start-from', type=int, default=0, help='Start processing from this record (default: 0)')
    parser.add_argument('--limit', type=int, help='Limit processing to N rows after start point (for testing)')
    parser.add_argument('--workers', type=int, default=MAX_WORKERS, help=f'Number of worker threads (default: {MAX_WORKERS})')
    parser.add_argument('--chunk-size', type=int, default=CHUNK_SIZE, help=f'Size of chunks to process (default: {CHUNK_SIZE})')
    args = parser.parse_args()
    
    # Update settings if provided
    if args.workers:
        MAX_WORKERS = args.workers
        logging.info(f"Using {MAX_WORKERS} worker threads")
    
    if args.chunk_size:
        CHUNK_SIZE = args.chunk_size
        logging.info(f"Using chunk size of {CHUNK_SIZE}")
    
    start_time = time.time()
    fix_all_records(start_from=args.start_from, limit=args.limit)
    elapsed_time = time.time() - start_time
    
    logging.info(f"Done! Total time: {elapsed_time:.2f} seconds") 