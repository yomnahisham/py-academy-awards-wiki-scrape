#!/usr/bin/env python3
import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import time
import random
from datetime import datetime
import os
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import gc
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)

# Configuration
INPUT_CSV = 'data/persons.csv'
OUTPUT_CSV = 'data/persons_fixed.csv'
RELATIONS_TABLES = ['data/nomination_person.csv', 'data/movie_crew.csv', 'data/award_edition_person.csv']
DELAY_BETWEEN_REQUESTS = 0.05  # Fixed small delay to be faster but still polite
MAX_WORKERS = 20  # Increased number of parallel threads
REQUEST_TIMEOUT = 3  # Shorter timeout for requests
CHUNK_SIZE = 500  # Process data in chunks to reduce memory usage
SAVE_INTERVAL = 1000  # Save intermediate results after processing this many rows

# Cache for Wikipedia requests - thread-safe dict
request_cache = {}
cache_lock = threading.Lock()

# Stats tracking - thread-safe counters
stats_lock = threading.Lock()
stats = {
    'updated_countries': 0,
    'updated_birthdates': 0,
    'updated_deathdates': 0,
    'skipped_persons': 0,
    'processed_rows': 0
}

def update_stats(key, value=1):
    with stats_lock:
        stats[key] += value

def clean_text(text):
    """Clean text by removing extra spaces, newlines, etc."""
    if not text:
        return ""
    return re.sub(r'\s+', ' ', text.strip())

def is_invalid_name(person_data):
    """Check if a person name is invalid (like '[1]', 'citation needed', 'emcee', etc.)"""
    name_parts = [
        person_data.get('first_name', ''),
        person_data.get('middle_name', ''),
        person_data.get('last_name', '')
    ]
    
    # Remove None and empty strings
    name_parts = [p for p in name_parts if p and not pd.isna(p)]
    
    # Check for invalid patterns
    invalid_patterns = [
        r'^\[\d+\]$',           # [1], [2], etc.
        r'^None$',              # Literal "None"
        r'^\d+$',               # Just numbers
        r'^citation needed$',   # "citation needed"
        r'^citation$',          # Just "citation"
        r'^needed$',            # Just "needed"
        r'^emcee$'              # "emcee"
    ]
    
    for part in name_parts:
        # Convert to string and lowercase for case-insensitive matching
        part_str = str(part).lower()
        
        # Direct check for specific invalid strings in any part
        for invalid_term in ["citation needed", "emcee"]:
            if invalid_term in part_str:
                return True
            
        # Check against regex patterns
        for pattern in invalid_patterns:
            if re.match(pattern, part_str):
                return True
    
    # Also check if entire name is unreasonably short
    full_name = ' '.join(name_parts).strip()
    if len(full_name) <= 1:
        return True
        
    return False

def remove_invalid_persons():
    """Remove invalid person entries and their relations from all tables"""
    logging.info("Checking for invalid person entries...")
    
    # Load persons table
    try:
        persons_df = pd.read_csv(INPUT_CSV)
        original_count = len(persons_df)
        
        # Find invalid entries more efficiently
        invalid_ids = []
        
        # Check for invalid entries in a more vectorized way if possible
        for idx, row in persons_df.iterrows():
            if is_invalid_name(row):
                invalid_ids.append(row['person_id'])
        
        if not invalid_ids:
            logging.info("No invalid person entries found.")
            return persons_df
            
        logging.info(f"Found {len(invalid_ids)} invalid person entries")
        
        # Process relations tables in parallel for speed
        def process_relation_file(relation_file):
            if os.path.exists(relation_file):
                try:
                    relations_df = pd.read_csv(relation_file)
                    original_relations = len(relations_df)
                    
                    # Filter out rows with invalid person_ids
                    relations_df = relations_df[~relations_df['person_id'].isin(invalid_ids)]
                    
                    # Save updated relations table
                    relations_df.to_csv(relation_file, index=False)
                    
                    removed = original_relations - len(relations_df)
                    return relation_file, removed
                except Exception as e:
                    logging.error(f"Error processing {relation_file}: {e}")
                    return relation_file, 0
            else:
                logging.warning(f"Relations file not found: {relation_file}")
                return relation_file, 0
        
        # Process relation files in parallel
        with ThreadPoolExecutor(max_workers=len(RELATIONS_TABLES)) as executor:
            futures = {executor.submit(process_relation_file, file): file for file in RELATIONS_TABLES}
            for future in as_completed(futures):
                file, removed = future.result()
                logging.info(f"Removed {removed} entries from {file}")
        
        # Remove invalid entries from persons dataframe
        persons_df = persons_df[~persons_df['person_id'].isin(invalid_ids)]
        
        removed = original_count - len(persons_df)
        logging.info(f"Removed {removed} invalid persons from persons table")
        
        return persons_df
        
    except Exception as e:
        logging.error(f"Error checking for invalid persons: {e}")
        import traceback
        traceback.print_exc()
        return pd.read_csv(INPUT_CSV)  # Return original on error

def get_soup(url, max_retries=2):
    """Get BeautifulSoup object from a URL with retries, using a cache"""
    # Check the cache first
    with cache_lock:
        if url in request_cache:
            return request_cache[url]
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    for attempt in range(max_retries):
        try:
            # Be polite but faster
            time.sleep(DELAY_BETWEEN_REQUESTS)
            
            response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'lxml')  # Using lxml for faster parsing
            
            # Cache the result
            with cache_lock:
                request_cache[url] = soup
                
            return soup
        except Exception as e:
            # Only print first error to reduce console spam
            if attempt == 0:
                logging.debug(f"Error fetching {url}: {e} (Attempt {attempt+1}/{max_retries})")
            # Short fixed backoff
            time.sleep(0.2)
    
    # Cache negative result too to avoid retrying
    with cache_lock:
        request_cache[url] = None
    
    return None

def normalize_date(date_str):
    """Normalize date strings to YYYY-MM-DD format"""
    if not date_str or pd.isna(date_str):
        return None
        
    date_str = str(date_str).strip()
    
    # Already in YYYY-MM-DD format
    if re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
        return date_str
        
    # Extract year at least which is the most common case
    year_match = re.search(r'\b(\d{4})\b', date_str)
    if year_match:
        return f"{year_match.group(1)}-01-01"
        
    return None

def build_wiki_url(person):
    """Build a Wikipedia URL based on person's name"""
    # Skip further processing if the name is invalid to save time
    if is_invalid_name(person):
        return None
        
    first_name = person.get('first_name', '')
    middle_name = person.get('middle_name', '')
    last_name = person.get('last_name', '')
    
    # Skip if no name parts available
    if all(pd.isna(n) or not n for n in [first_name, middle_name, last_name]):
        return None
    
    # Combine names, handling empty parts
    name_parts = [part for part in [first_name, middle_name, last_name] if part and not pd.isna(part)]
    if not name_parts:
        return None
        
    full_name = "_".join(name_parts)
    
    # Clean the name for URL use
    full_name = full_name.replace(' ', '_')
    
    # Base Wikipedia URL
    base_url = "https://en.wikipedia.org/wiki/"
    
    return base_url + full_name

def extract_person_details(url):
    """Extract person details from a Wikipedia page"""
    if not url:
        return {}
        
    soup = get_soup(url)
    if not soup:
        return {}
    
    details = {
        'country': None,
        'birth_date': None,
        'death_date': None
    }
    
    try:
        # Find infobox
        infobox = soup.find('table', {'class': 'infobox'})
        if not infobox:
            return details
            
        # Extract country from Born field
        born_row = None
        for row in infobox.find_all('tr'):
            header = row.find('th')
            if not header:
                continue
                
            header_text = header.get_text().lower()
            
            # Look specifically for "Born" field
            if 'born' in header_text:
                born_row = row
                born_data = row.find('td')
                if born_data:
                    # Extract birth date
                    born_text = clean_text(born_data.get_text())
                    # Extract year from birth date
                    year_match = re.search(r'\b(\d{4})\b', born_text)
                    if year_match:
                        details['birth_date'] = year_match.group(1) + "-01-01"
                    
                    # Extract country - it's typically after the date in the Born field
                    # Look for country names after a year
                    countries_to_check = [
                        "United States", "UK", "U.S.", "USA", "U.S.A.", "America", 
                        "United Kingdom", "Great Britain", "England", "Scotland", "Wales",
                        "Ireland", "France", "Germany", "Italy", "Canada", "Australia",
                        "Japan", "China", "Russia", "India", "Brazil", "Mexico"
                    ]
                    
                    # First try to find any of these countries in the born text
                    for country in countries_to_check:
                        if country in born_text:
                            details['country'] = country
                            break
                    
                    # If no country found, look for common nationality terms
                    if not details['country']:
                        nationality_mapping = {
                            "american": "United States",
                            "british": "United Kingdom", 
                            "english": "United Kingdom",
                            "scottish": "United Kingdom",
                            "welsh": "United Kingdom",
                            "irish": "Ireland",
                            "french": "France",
                            "german": "Germany",
                            "italian": "Italy",
                            "canadian": "Canada", 
                            "australian": "Australia",
                            "japanese": "Japan",
                            "chinese": "China",
                            "russian": "Russia",
                            "indian": "India",
                            "brazilian": "Brazil",
                            "mexican": "Mexico"
                        }
                        
                        lower_born_text = born_text.lower()
                        for nationality, country in nationality_mapping.items():
                            if nationality in lower_born_text:
                                details['country'] = country
                                break
                    
                    # If still no country, just take the last part after comma which often contains location
                    if not details['country'] and ',' in born_text:
                        parts = born_text.split(',')
                        # Take the last non-empty part
                        for part in reversed(parts):
                            if part.strip():
                                details['country'] = part.strip()
                                break
            
            # Look for death date
            if 'died' in header_text or 'death' in header_text:
                data = row.find('td')
                if data:
                    # Try to find a date
                    date_text = clean_text(data.get_text())
                    # Extract year at minimum
                    year_match = re.search(r'\b(\d{4})\b', date_text)
                    if year_match:
                        details['death_date'] = year_match.group(1) + "-01-01"
    except Exception as e:
        logging.debug(f"Error extracting details from {url}: {e}")
    
    return details

def normalize_country(country_str):
    """
    Normalize country strings to standardized country names.
    Handles multiple formats, historical references, and US states.
    """
    if not country_str or pd.isna(country_str):
        return None
        
    # Clean the input
    country_str = str(country_str).strip()
    
    # Standard country name mappings
    country_mappings = {
        # United States variations
        "u.s.": "United States",
        "u.s.a.": "United States",
        "usa": "United States",
        "america": "United States",
        "united states of america": "United States",
        "american": "United States",
        
        # United Kingdom variations
        "u.k.": "United Kingdom",
        "uk": "United Kingdom",
        "great britain": "United Kingdom",
        "england": "United Kingdom",
        "scotland": "United Kingdom",
        "wales": "United Kingdom",
        "northern ireland": "United Kingdom",
        "britain": "United Kingdom",
        "british": "United Kingdom",
        "english": "United Kingdom",
        "scottish": "United Kingdom",
        "welsh": "United Kingdom",
        
        # Other country variations
        "ussr": "Russia",
        "soviet union": "Russia",
        "russian": "Russia",
        
        "republic of ireland": "Ireland",
        "irish": "Ireland",
        
        "federal republic of germany": "Germany",
        "west germany": "Germany",
        "east germany": "Germany",
        "nazi germany": "Germany",
        "german": "Germany",
        
        "people's republic of china": "China",
        "republic of china": "China",
        "chinese": "China",
        
        "japan": "Japan",
        "japanese": "Japan",
        
        "france": "France",
        "french": "France",
        "french republic": "France",
        
        "italy": "Italy",
        "italian": "Italy",
        "italian republic": "Italy",
        
        "canada": "Canada",
        "canadian": "Canada",
        
        "australia": "Australia",
        "commonwealth of australia": "Australia",
        "australian": "Australia",
        
        "india": "India",
        "republic of india": "India",
        "indian": "India",
        
        "brazil": "Brazil",
        "brazilian": "Brazil",
        "federative republic of brazil": "Brazil",
        
        "mexico": "Mexico",
        "mexican": "Mexico",
        "united mexican states": "Mexico",
        
        "spain": "Spain",
        "kingdom of spain": "Spain",
        "spanish": "Spain",
        
        "netherlands": "Netherlands",
        "holland": "Netherlands",
        "dutch": "Netherlands",
        "kingdom of the netherlands": "Netherlands",
        
        "sweden": "Sweden",
        "kingdom of sweden": "Sweden",
        "swedish": "Sweden",
        
        "norway": "Norway",
        "kingdom of norway": "Norway",
        "norwegian": "Norway",
        
        "denmark": "Denmark",
        "kingdom of denmark": "Denmark",
        "danish": "Denmark",
        
        "poland": "Poland",
        "republic of poland": "Poland",
        "polish": "Poland",
        
        "austria": "Austria",
        "republic of austria": "Austria",
        "austrian": "Austria",
        
        "switzerland": "Switzerland",
        "swiss": "Switzerland",
        "swiss confederation": "Switzerland",
        
        "belgium": "Belgium",
        "kingdom of belgium": "Belgium",
        "belgian": "Belgium",
        
        "greece": "Greece",
        "hellenic republic": "Greece",
        "greek": "Greece",
        
        "turkey": "Turkey",
        "republic of turkey": "Turkey",
        "turkish": "Turkey",
        
        "egypt": "Egypt",
        "arab republic of egypt": "Egypt",
        "kingdom of egypt": "Egypt",
        "egyptian": "Egypt",
        
        "south africa": "South Africa",
        "republic of south africa": "South Africa",
        "south african": "South Africa",
        
        # Add more countries as needed
    }
    
    # US states that should be mapped to United States
    us_states = [
        "alabama", "alaska", "arizona", "arkansas", "california", 
        "colorado", "connecticut", "delaware", "florida", "georgia", 
        "hawaii", "idaho", "illinois", "indiana", "iowa", "kansas", 
        "kentucky", "louisiana", "maine", "maryland", "massachusetts", 
        "michigan", "minnesota", "mississippi", "missouri", "montana", 
        "nebraska", "nevada", "new hampshire", "new jersey", "new mexico", 
        "new york", "north carolina", "north dakota", "ohio", "oklahoma", 
        "oregon", "pennsylvania", "rhode island", "south carolina", 
        "south dakota", "tennessee", "texas", "utah", "vermont", 
        "virginia", "washington", "west virginia", "wisconsin", "wyoming",
        # Include common city mentions that should map to US
        "hollywood", "los angeles", "new york city", "chicago", "boston",
        "san francisco", "washington d.c.", "washington dc"
    ]
    
    # First, handle parentheticals by taking only the text before parentheses
    if "(" in country_str:
        country_str = country_str.split("(")[0].strip()
    
    # Handle multi-country strings by taking the first one
    if "," in country_str:
        parts = [p.strip() for p in country_str.split(",")]
        country_str = parts[0]
    
    # Convert to lowercase for case-insensitive matching
    country_lower = country_str.lower()
    
    # Check for exact matches in the mapping
    if country_lower in country_mappings:
        return country_mappings[country_lower]
    
    # Check if the string contains a US state name
    for state in us_states:
        if state in country_lower:
            return "United States"
    
    # Check for partial matches in country names
    for key, value in country_mappings.items():
        if key in country_lower:
            return value
            
    # For any other unmatched case, if it's a recognized country name, keep it
    recognized_countries = set(country_mappings.values())
    if country_str in recognized_countries:
        return country_str
        
    # Last resort: return as is but log that we couldn't normalize it
    logging.debug(f"Could not normalize country name: {country_str}")
    return country_str

def process_person(row_tuple):
    """Process a single person record (thread-safe)"""
    idx, row, total_rows = row_tuple
    person_data = row.to_dict()
    
    # Skip processing invalid persons to save time
    if is_invalid_name(person_data):
        update_stats('skipped_persons')
        return None
    
    # Create a copy of the row to update
    updated_row = row.copy()
    
    # Build Wikipedia URL and get details
    wiki_url = build_wiki_url(person_data)
    
    # If no valid Wikipedia page found, skip this person
    if wiki_url is None:
        update_stats('skipped_persons')
        return updated_row
    
    details = extract_person_details(wiki_url)
    
    # Update country if found
    if details.get('country') and details['country']:
        # Normalize the country to just the birth country
        country = normalize_country(details['country'])
        if country:
            updated_row['country'] = country
            update_stats('updated_countries')
        
    # Update birth date if found
    if details.get('birth_date') and (pd.isna(updated_row['birthDate']) if 'birthDate' in updated_row else True):
        updated_row['birthDate'] = str(details['birth_date'])
        update_stats('updated_birthdates')
        
    # Update death date if found
    if details.get('death_date') and (pd.isna(updated_row['deathDate']) if 'deathDate' in updated_row else True):
        updated_row['deathDate'] = str(details['death_date'])
        update_stats('updated_deathdates')
    
    # Update progress counter and print progress
    update_stats('processed_rows')
    
    # Print progress periodically
    processed = stats['processed_rows']
    if processed % 100 == 0 or processed == total_rows:
        percent = processed / total_rows * 100
        logging.info(f"Processed {processed}/{total_rows} ({percent:.1f}%) - Updated: {stats['updated_countries']} countries, {stats['updated_birthdates']} birth dates")
    
    return updated_row

def process_chunk(chunk_df, start_idx, total_rows):
    """Process a chunk of the dataframe in parallel"""
    process_args = [(start_idx + i, row, total_rows) for i, (_, row) in enumerate(chunk_df.iterrows())]
    
    updated_rows = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_person, arg) for arg in process_args]
        for future in as_completed(futures):
            try:
                result = future.result()
                if result is not None:
                    updated_rows.append(result)
            except Exception as e:
                logging.error(f"Error processing person: {e}")
    
    return updated_rows

def fix_persons_csv(limit=None):
    """
    Main function to fix the persons.csv file - processes data in chunks to reduce memory usage
    """
    logging.info(f"Reading input CSV: {INPUT_CSV}")
    
    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
    
    try:
        # First remove invalid person entries and their relations
        df = remove_invalid_persons()
        
        total_rows = len(df)
        logging.info(f"Processing {total_rows} valid rows from {INPUT_CSV}")
        
        # Limit the number of rows if requested
        if limit and limit > 0 and limit < total_rows:
            logging.info(f"Limiting to first {limit} rows for testing")
            df = df.iloc[:limit].copy()
            total_rows = len(df)
        
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
        
        # Split data into chunks
        for chunk_start in range(0, total_rows, CHUNK_SIZE):
            chunk_end = min(chunk_start + CHUNK_SIZE, total_rows)
            chunk_df = df.iloc[chunk_start:chunk_end].copy()
            
            logging.info(f"Processing chunk {chunk_start}-{chunk_end} of {total_rows}")
            
            # Process the chunk
            updated_rows = process_chunk(chunk_df, chunk_start, total_rows)
            all_updated_rows.extend(updated_rows)
            
            # Save intermediate results periodically
            if len(all_updated_rows) >= SAVE_INTERVAL:
                temp_df = pd.DataFrame(all_updated_rows)
                if not temp_df.empty:
                    interim_file = f"{OUTPUT_CSV}.interim"
                    temp_df[output_columns].to_csv(interim_file, index=False)
                    logging.info(f"Saved interim results ({len(all_updated_rows)} rows) to {interim_file}")
                
                # Free memory
                all_updated_rows = []
                gc.collect()
        
        # Create final output DataFrame
        output_df = pd.DataFrame(all_updated_rows)
        
        # If we've been saving interim results, read and combine them
        interim_file = f"{OUTPUT_CSV}.interim"
        if os.path.exists(interim_file):
            interim_df = pd.read_csv(interim_file)
            output_df = pd.concat([interim_df, output_df], ignore_index=True)
            os.remove(interim_file)  # Clean up interim file
        
        # Keep only the columns we want and save to output
        if not output_df.empty:
            # Keep only the specified columns and save to output
            output_df = output_df[output_columns].copy()
            output_df.to_csv(OUTPUT_CSV, index=False)
            
            logging.info(f"\nSummary of fixes:")
            logging.info(f"- Updated countries: {stats['updated_countries']}")
            logging.info(f"- Updated birth dates: {stats['updated_birthdates']}")
            logging.info(f"- Updated death dates: {stats['updated_deathdates']}")
            logging.info(f"- Skipped persons: {stats['skipped_persons']}")
            logging.info(f"Fixed data saved to: {OUTPUT_CSV}")
        else:
            logging.warning("No rows were processed successfully")
        
    except Exception as e:
        logging.error(f"Error processing CSV: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    return True

if __name__ == "__main__":
    logging.info("Starting persons.csv fix script...")
    
    parser = argparse.ArgumentParser(description='Fix persons.csv by removing invalid entries and fetching missing data from Wikipedia')
    parser.add_argument('--limit', type=int, help='Limit processing to the first N rows (for testing)')
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
    
    # Use limit if provided or None for full processing
    limit = args.limit if args.limit else None
    
    start_time = time.time()
    fix_persons_csv(limit=limit)
    elapsed_time = time.time() - start_time
    
    logging.info(f"Done! Total time: {elapsed_time:.2f} seconds")