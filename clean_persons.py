#!/usr/bin/env python3
import pandas as pd
import re

def clean_name_field(name):
    """Clean a name field by removing extra commas and quotes"""
    if pd.isna(name):
        return name
    
    # Remove quotes
    name = str(name).replace('"', '')
    
    # Remove trailing commas
    name = name.rstrip(',')
    
    return name

def clean_venue_name(name):
    """Clean and truncate venue name to fit in 60 characters"""
    if pd.isna(name):
        return name
    
    name = str(name)
    
    # Remove location qualifiers if name is too long
    if len(name) > 60:
        # Remove common location phrases
        patterns = [
            r' In Los Angeles On March \d+',
            r' In Hollywood',
            r' In Beverly Hills',
            r' Complex In Hollywood',
            r' Of The Ovation Hollywood Complex In Hollywood'
        ]
        for pattern in patterns:
            name = re.sub(pattern, '', name)
    
    # If still too long, truncate
    if len(name) > 60:
        name = name[:57] + '...'
    
    return name

def is_movie_title(row):
    """Check if a row appears to be a movie title rather than a person"""
    # Common indicators of movie titles
    movie_indicators = [
        row['first_name'] == 'The',
        'Are Coming' in str(row['middle_name']),
        'Dragon' in str(row['last_name']),
        'Tiger' in str(row['middle_name']),
        'Billboards' in str(row['middle_name']),
        'Legend' in str(row['middle_name']),
        'Brother' in str(row['middle_name']),
        'Empire Strikes' in str(row['middle_name']),
    ]
    return any(movie_indicators)

def clean_persons():
    """Clean and validate the persons data"""
    print("Loading persons_fixed.csv...")
    # Read only the expected columns
    df = pd.read_csv('data/persons_fixed.csv', usecols=['person_id', 'first_name', 'middle_name', 'last_name', 'birthDate', 'country', 'deathDate'])
    print(f"Loaded {len(df)} rows")
    
    # Clean name fields
    print("Cleaning name fields...")
    df['first_name'] = df['first_name'].apply(clean_name_field)
    df['middle_name'] = df['middle_name'].apply(clean_name_field)
    df['last_name'] = df['last_name'].apply(clean_name_field)
    
    # Remove movie titles
    print("Removing movie titles...")
    original_len = len(df)
    df = df[~df.apply(is_movie_title, axis=1)]
    removed_count = original_len - len(df)
    print(f"Removed {removed_count} movie titles")
    
    # Handle missing last names
    print("Handling missing last names...")
    # If last_name is null but middle_name exists, move middle_name to last_name
    mask = df['last_name'].isna() & df['middle_name'].notna()
    df.loc[mask, 'last_name'] = df.loc[mask, 'middle_name']
    df.loc[mask, 'middle_name'] = None
    
    # If still no last_name, use first_name as last_name
    mask = df['last_name'].isna()
    df.loc[mask, 'last_name'] = df.loc[mask, 'first_name']
    
    # Remove rows that still have null last_name
    original_len = len(df)
    df = df[df['last_name'].notna()]
    removed_count = original_len - len(df)
    print(f"Removed {removed_count} rows with null last names")
    
    # Save cleaned data
    print("Saving cleaned persons data...")
    df.to_csv('data/persons_fixed_clean.csv', index=False)
    print(f"Saved {len(df)} rows to persons_fixed_clean.csv")

def clean_venues():
    """Clean and validate the venues data"""
    print("\nLoading venues.csv...")
    df = pd.read_csv('data/venues.csv')
    print(f"Loaded {len(df)} rows")
    
    # Clean venue names
    print("Cleaning venue names...")
    df['venue_name'] = df['venue_name'].apply(clean_venue_name)
    
    # Save cleaned data
    print("Saving cleaned venues data...")
    df.to_csv('data/venues_clean.csv', index=False)
    print(f"Saved {len(df)} rows to venues_clean.csv")

def main():
    clean_persons()
    clean_venues()

if __name__ == "__main__":
    main() 