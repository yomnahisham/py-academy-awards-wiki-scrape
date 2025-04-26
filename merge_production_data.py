#!/usr/bin/env python3
"""
Merge production company data from checking_data into data directory.
Only updates production_company.csv and movie_produced_by.csv in the data directory.
"""

import pandas as pd
import numpy as np
import re

def clean_movie_name(name):
    """Clean movie name by removing year and film suffix"""
    # Remove year in parentheses if present
    name = re.sub(r'\s*\(\d{4}\)', '', name)
    # Remove "film" suffix if present
    name = re.sub(r'\s*\(film\)', '', name)
    return name.strip()

def load_and_clean_df(filepath):
    """Load CSV and clean basic data issues"""
    df = pd.read_csv(filepath)
    # Replace empty strings and NaN with None
    df = df.replace({np.nan: None, '': None})
    return df

def main():
    print("Loading files...")
    try:
        # Read the deemovie production companies data
        deemovie_df = pd.read_csv('checking_data/deemovie_production_companies.csv')
        print(f"Loaded {len(deemovie_df)} rows from deemovie production companies")
        
        # Read our existing movies data
        movies_df = pd.read_csv('data/movies.csv')
        # Handle duplicate run_time columns if they exist
        if 'run_time.1' in movies_df.columns:
            movies_df = movies_df.drop('run_time.1', axis=1)
        print(f"Loaded {len(movies_df)} rows from our movies")
        
        # Read our existing production companies and relationships
        prod_companies_df = pd.read_csv('data/production_company.csv')
        print(f"Loaded {len(prod_companies_df)} rows from our production companies")
        
        existing_relationships_df = pd.read_csv('data/movie_produced_by.csv')
        print(f"Loaded {len(existing_relationships_df)} existing relationships")
        
        # Clean movie names in both datasets
        print("\nCleaning movie names...")
        deemovie_df['clean_name'] = deemovie_df['film_name'].apply(clean_movie_name)
        movies_df['clean_name'] = movies_df['movie_name'].apply(clean_movie_name)
        
        # Create a mapping of production company names to IDs
        company_name_to_id = dict(zip(prod_companies_df['company_name'], prod_companies_df['pd_id']))
        next_company_id = max(company_name_to_id.values()) + 1 if company_name_to_id else 1
        
        # Create a mapping of movie names to IDs
        movie_name_to_id = dict(zip(movies_df['clean_name'], movies_df['movie_id']))
        
        # Create set of existing relationships
        existing_relationships = set(zip(existing_relationships_df['movie_id'], 
                                      existing_relationships_df['production_company_id']))
        
        # Prepare new production companies and relationships
        new_companies = []
        new_relationships = []
        skipped_movies = []
        
        print("Processing relationships...")
        # Process each row in deemovie data
        for _, row in deemovie_df.iterrows():
            movie_name = row['clean_name']
            company_name = row['production_company']
            
            # Skip if movie not found in our dataset
            if movie_name not in movie_name_to_id:
                skipped_movies.append(row['film_name'])
                continue
                
            movie_id = movie_name_to_id[movie_name]
            
            # Skip if company name is None or empty
            if not company_name or pd.isna(company_name):
                continue
                
            # If company doesn't exist, add it
            if company_name not in company_name_to_id:
                company_name_to_id[company_name] = next_company_id
                new_companies.append({
                    'pd_id': next_company_id,
                    'company_name': company_name
                })
                next_company_id += 1
                
            company_id = company_name_to_id[company_name]
            
            # Add relationship if it doesn't exist
            if (movie_id, company_id) not in existing_relationships:
                new_relationships.append({
                    'movie_id': movie_id,
                    'production_company_id': company_id
                })
        
        # Create DataFrames for new data
        new_companies_df = pd.DataFrame(new_companies)
        new_relationships_df = pd.DataFrame(new_relationships)
        
        # Remove duplicates
        new_companies_df = new_companies_df.drop_duplicates(subset=['company_name'])
        new_relationships_df = new_relationships_df.drop_duplicates()
        
        print("\nSaving results...")
        # Save new production companies
        if not new_companies_df.empty:
            # Combine existing and new companies, ensuring no duplicates
            updated_prod_companies = pd.concat([prod_companies_df, new_companies_df], ignore_index=True)
            updated_prod_companies = updated_prod_companies.drop_duplicates(subset=['company_name'])
            updated_prod_companies.to_csv('data/production_company.csv', index=False)
            print(f"Added {len(new_companies_df)} new production companies")
        else:
            print("No new production companies to add")
        
        # Save new relationships
        if not new_relationships_df.empty:
            # Combine existing and new relationships, ensuring no duplicates
            updated_relationships = pd.concat([existing_relationships_df, new_relationships_df], ignore_index=True)
            updated_relationships = updated_relationships.drop_duplicates()
            updated_relationships.to_csv('data/movie_produced_by.csv', index=False)
            print(f"Added {len(new_relationships_df)} new movie-production company relationships")
        else:
            print("No new movie-production company relationships to add")
        
        print(f"\nSkipped {len(skipped_movies)} movies that were not found in our dataset")
        if len(skipped_movies) > 0:
            print("First 5 skipped movies:", skipped_movies[:5])
            
    except FileNotFoundError as e:
        print(f"Error: Could not find file - {str(e)}")
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    main() 