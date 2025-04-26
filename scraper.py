import requests
import pandas as pd
import re
import time
import os
import csv
from datetime import datetime
from bs4 import BeautifulSoup
from tqdm import tqdm
import random
from urllib.parse import urljoin

# Create directories for CSVs
os.makedirs('data', exist_ok=True)

BASE_URL = 'https://en.wikipedia.org/wiki/Academy_Award_for_'

# Categories to scrape
AWARDS = [
    'Best_Makeup_and_Hairstyling', 'Best_Documentary_Feature_Film',
    'Best_Original_Score', 'Best_Original_Song',
    'Best_Documentary_Short_Film', 'Best_Picture',
    'Best_Animated_Feature', 'Best_Visual_Effects',
    'Best_Adapted_Screenplay', 'Best_Film_Editing',
    'Best_Production_Design', 'Best_Animated_Short_Film',
    'Best_Sound'
]

# Map award to category IDs
CATEGORY_MAP = {
    'Best_Makeup_and_Hairstyling': 1,
    'Best_Documentary_Feature_Film': 2,
    'Best_Original_Score': 3,
    'Best_Original_Song': 4,
    'Best_Documentary_Short_Film': 5,
    'Best_Picture': 6,
    'Best_Animated_Feature': 7,
    'Best_Visual_Effects': 8, 
    'Best_Adapted_Screenplay': 9,
    'Best_Film_Editing': 10,
    'Best_Production_Design': 11,
    'Best_Animated_Short_Film': 12,
    'Best_Sound': 13
}

# Map position titles to IDs
POSITION_MAP = {
    'Director': 1,
    'Actor': 2,
    'Actress': 3,
    'Supporting Actor': 4,
    'Supporting Actress': 5,
    'Producer': 6,
    'Writer': 7,
    'Editor': 8,
    'Cinematographer': 9,
    'Composer': 10,
    'Singer': 11,
    'Art Director': 12,
    'Visual Effects Artist': 13,
    'Makeup Artist': 14,
    'Hairstylist': 15,
    'Sound Designer': 16,
    'Animator': 17,
    'Host': 18,
    'Presenter': 19
}

# Initialize dataframes for each table
venues_df = pd.DataFrame(columns=['venue_id', 'venue_name', 'neighborhood', 'city', 'state', 'country'])
award_editions_df = pd.DataFrame(columns=['award_edition_id', 'edition', 'aYear', 'cDate', 'venue_id', 'duration', 'network'])
positions_df = pd.DataFrame(columns=['position_id', 'title'])
persons_df = pd.DataFrame(columns=['person_id', 'first_name', 'middle_name', 'last_name', 'birthDate', 'country', 'deathDate'])
award_edition_person_df = pd.DataFrame(columns=['award_edition_id', 'person_id', 'position_id'])
categories_df = pd.DataFrame(columns=['category_id', 'category_name'])
movies_df = pd.DataFrame(columns=['movie_id', 'movie_name', 'run_time'])
movie_language_df = pd.DataFrame(columns=['movie_id', 'in_language'])
movie_release_date_df = pd.DataFrame(columns=['movie_id', 'release_date'])
movie_country_df = pd.DataFrame(columns=['movie_id', 'country'])
movie_crew_df = pd.DataFrame(columns=['movie_id', 'person_id', 'position_id'])
production_company_df = pd.DataFrame(columns=['pd_id', 'company_name'])
movie_produced_by_df = pd.DataFrame(columns=['movie_id', 'production_company_id'])
nominations_df = pd.DataFrame(columns=['nomination_id', 'award_edition_id', 'movie_id', 'category_id', 'won', 'submitted_by'])
nomination_person_df = pd.DataFrame(columns=['nomination_id', 'person_id'])

# Add positions data
for position_id, title in POSITION_MAP.items():
    positions_df = pd.concat([positions_df, pd.DataFrame({'position_id': [title], 'title': [position_id]})], ignore_index=True)

# Add categories data
for category_name, category_id in CATEGORY_MAP.items():
    category_name = category_name.replace('_', ' ')
    categories_df = pd.concat([categories_df, pd.DataFrame({'category_id': [category_id], 'category_name': [category_name]})], ignore_index=True)

# Helper functions
def get_soup(url):
    """Get BeautifulSoup object from URL with retries and random delays"""
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
    
    for attempt in range(5):
        try:
            time.sleep(random.uniform(1, 3))  # Random delay to be polite
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return BeautifulSoup(response.text, 'html.parser')
        except Exception as e:
            print(f"Error fetching {url}: {e}. Attempt {attempt+1}/5")
            time.sleep(2 * attempt)  # Exponential backoff
    
    print(f"Failed to fetch {url} after 5 attempts")
    return None

def parse_person_name(name):
    """Parse person name into first, middle, and last names"""
    if not name:
        return None, None, None
    
    # Remove parentheses content
    name = re.sub(r'\([^)]*\)', '', name).strip()
    
    # Split name by spaces
    parts = name.split()
    
    if len(parts) == 1:
        return parts[0], None, ""
    elif len(parts) == 2:
        return parts[0], None, parts[1]
    else:
        return parts[0], ' '.join(parts[1:-1]), parts[-1]

def clean_text(text):
    """Clean text by removing newlines, extra spaces, etc."""
    if not text:
        return ""
    return re.sub(r'\s+', ' ', text.strip())

def extract_year(text):
    """Extract year from text"""
    match = re.search(r'(\d{4})', text)
    if match:
        return match.group(1)
    return None

def add_person(name):
    """Add person to persons_df if not already there and return person_id"""
    global persons_df
    
    first_name, middle_name, last_name = parse_person_name(name)
    
    if not first_name:
        return None
    
    # Check if person already exists
    existing = persons_df[(persons_df['first_name'] == first_name) & 
                         (persons_df['last_name'] == last_name)]
    
    if not existing.empty:
        return existing.iloc[0]['person_id']
    
    # Add new person
    new_person_id = len(persons_df) + 1
    
    new_person = {
        'person_id': new_person_id,
        'first_name': first_name,
        'middle_name': middle_name,
        'last_name': last_name,
        'birthDate': None,
        'country': None,
        'deathDate': None
    }
    
    persons_df = pd.concat([persons_df, pd.DataFrame([new_person])], ignore_index=True)
    return new_person_id

def add_movie(title, run_time=None):
    """Add movie to movies_df if not already there and return movie_id"""
    global movies_df
    
    title = clean_text(title)
    
    if not title:
        return None
    
    # Check if movie already exists
    existing = movies_df[movies_df['movie_name'] == title]
    
    if not existing.empty:
        return existing.iloc[0]['movie_id']
    
    # Add new movie
    new_movie_id = len(movies_df) + 1
    
    new_movie = {
        'movie_id': new_movie_id,
        'movie_name': title,
        'run_time': run_time
    }
    
    movies_df = pd.concat([movies_df, pd.DataFrame([new_movie])], ignore_index=True)
    return new_movie_id

def add_nomination(award_edition_id, movie_id, category_id, won=False):
    """Add nomination to nominations_df and return nomination_id"""
    global nominations_df
    
    if not movie_id or not award_edition_id:
        return None
    
    # Add new nomination
    new_nomination_id = len(nominations_df) + 1
    
    new_nomination = {
        'nomination_id': new_nomination_id,
        'award_edition_id': award_edition_id,
        'movie_id': movie_id,
        'category_id': category_id,
        'won': 1 if won else 0,
        'submitted_by': None
    }
    
    nominations_df = pd.concat([nominations_df, pd.DataFrame([new_nomination])], ignore_index=True)
    return new_nomination_id

def add_venue(venue_name, city=None, state="California", country="U.S."):
    """Add venue to venues_df if not already there and return venue_id"""
    global venues_df
    
    venue_name = clean_text(venue_name)
    
    if not venue_name:
        return None
    
    # Check if venue already exists
    existing = venues_df[venues_df['venue_name'] == venue_name]
    
    if not existing.empty:
        return existing.iloc[0]['venue_id']
    
    # Add new venue
    new_venue_id = len(venues_df) + 1
    
    new_venue = {
        'venue_id': new_venue_id,
        'venue_name': venue_name,
        'neighborhood': None,
        'city': city,
        'state': state,
        'country': country
    }
    
    venues_df = pd.concat([venues_df, pd.DataFrame([new_venue])], ignore_index=True)
    return new_venue_id

def add_award_edition(edition, year, ceremony_date=None, venue_id=None, network=None):
    """Add award edition to award_editions_df if not already there and return award_edition_id"""
    global award_editions_df
    
    if not edition:
        return None
    
    # Check if award edition already exists
    existing = award_editions_df[award_editions_df['edition'] == edition]
    
    if not existing.empty:
        return existing.iloc[0]['award_edition_id']
    
    # Add new award edition
    new_award_edition_id = len(award_editions_df) + 1
    
    new_award_edition = {
        'award_edition_id': new_award_edition_id,
        'edition': edition,
        'aYear': year,
        'cDate': ceremony_date,
        'venue_id': venue_id,
        'duration': None,
        'network': network
    }
    
    award_editions_df = pd.concat([award_editions_df, pd.DataFrame([new_award_edition])], ignore_index=True)
    return new_award_edition_id

def process_category_page(category_name, category_id):
    """Process a single category page and extract all nominations"""
    print(f"Processing {category_name}...")
    
    url = f"{BASE_URL}{category_name}"
    soup = get_soup(url)
    
    if not soup:
        print(f"Failed to fetch {url}")
        return
    
    # Find all tables that might contain nominations
    tables = soup.find_all('table', class_='wikitable')
    
    if not tables:
        print(f"No tables found for {category_name}")
        return
    
    nominations_count = 0
    
    # Process each table
    for table in tables:
        # Look for year information in table headers
        year_match = None
        edition_match = None
        
        # Check for ceremony information
        prev_headers = table.find_previous(['h2', 'h3', 'h4'])
        if prev_headers:
            header_text = prev_headers.get_text()
            year_match = re.search(r'(\d{1,2})(st|nd|rd|th) Academy Awards \((\d{4})\)', header_text)
            
            if year_match:
                edition = int(year_match.group(1))
                ceremony_year = int(year_match.group(3))
                # Default venue - will be filled with proper data in a real scraper
                venue_id = add_venue("Dolby Theatre", "Hollywood")
                award_edition_id = add_award_edition(edition, ceremony_year, None, venue_id, "ABC")
        
        # Process rows
        rows = table.find_all('tr')
        
        # Skip header row
        for row in rows[1:]:
            cells = row.find_all(['td', 'th'])
            
            if len(cells) < 2:  # Need at least film and person/status
                continue
            
            film_cell = cells[0]
            
            # Extract film information
            film_link = film_cell.find('a')
            if film_link:
                film_title = clean_text(film_link.get_text())
                film_id = add_movie(film_title)
            else:
                film_title = clean_text(film_cell.get_text())
                film_id = add_movie(film_title)
            
            # Determine if won (usually bold or italic text indicates winning)
            won = bool(film_cell.find(['b', 'strong', 'i', 'em']) or 
                       'background:#FAEB86' in str(row) or 
                       'background-color:#FAEB86' in str(row))
            
            # People involved
            people_cell = cells[1] if len(cells) > 1 else None
            
            if people_cell:
                person_links = people_cell.find_all('a')
                
                if person_links:
                    for person_link in person_links:
                        person_name = clean_text(person_link.get_text())
                        person_id = add_person(person_name)
                        
                        if person_id and film_id and award_edition_id:
                            # Add nomination
                            nomination_id = add_nomination(award_edition_id, film_id, category_id, won)
                            
                            # Add nomination-person relationship
                            if nomination_id:
                                nomination_person_df = pd.concat([
                                    nomination_person_df, 
                                    pd.DataFrame({
                                        'nomination_id': [nomination_id],
                                        'person_id': [person_id]
                                    })
                                ], ignore_index=True)
                                
                                # For simplicity, associate with appropriate position based on category
                                position_id = 1  # Default to Director
                                if "Actor" in category_name:
                                    position_id = 2
                                elif "Actress" in category_name:
                                    position_id = 3
                                
                                # Add to movie_crew
                                movie_crew_df = pd.concat([
                                    movie_crew_df,
                                    pd.DataFrame({
                                        'movie_id': [film_id],
                                        'person_id': [person_id],
                                        'position_id': [position_id]
                                    })
                                ], ignore_index=True)
                                
                                nominations_count += 1
                else:
                    # No specific people mentioned, just add the nomination
                    if film_id and award_edition_id:
                        nomination_id = add_nomination(award_edition_id, film_id, category_id, won)
                        nominations_count += 1
    
    print(f"Extracted {nominations_count} nominations for {category_name}")

# Main execution
def main():
    # Add common venues and networks before starting
    dolby_theatre = add_venue("Dolby Theatre", "Hollywood")
    kodak_theatre = add_venue("Kodak Theatre", "Hollywood")
    shrine_auditorium = add_venue("Shrine Auditorium", "Los Angeles")
    
    # Process each award category
    for award in tqdm(AWARDS):
        category_id = CATEGORY_MAP[award]
        process_category_page(award, category_id)
        # Pause between categories to be respectful to Wikipedia
        time.sleep(random.uniform(2, 5))
    
    # Save dataframes to CSV
    print("Saving data to CSV files...")
    
    # Save all dataframes to CSV
    venues_df.to_csv('data/venues.csv', index=False)
    award_editions_df.to_csv('data/award_editions.csv', index=False)
    positions_df.to_csv('data/positions.csv', index=False)
    persons_df.to_csv('data/persons.csv', index=False)
    award_edition_person_df.to_csv('data/award_edition_person.csv', index=False)
    categories_df.to_csv('data/categories.csv', index=False)
    movies_df.to_csv('data/movies.csv', index=False)
    movie_language_df.to_csv('data/movie_language.csv', index=False)
    movie_release_date_df.to_csv('data/movie_release_date.csv', index=False)
    movie_country_df.to_csv('data/movie_country.csv', index=False)
    movie_crew_df.to_csv('data/movie_crew.csv', index=False)
    production_company_df.to_csv('data/production_company.csv', index=False)
    movie_produced_by_df.to_csv('data/movie_produced_by.csv', index=False)
    nominations_df.to_csv('data/nominations.csv', index=False)
    nomination_person_df.to_csv('data/nomination_person.csv', index=False)
    
    print(f"Total nominations: {len(nominations_df)}")
    print(f"Total movies: {len(movies_df)}")
    print(f"Total persons: {len(persons_df)}")
    print("Done!")

if __name__ == "__main__":
    main() 