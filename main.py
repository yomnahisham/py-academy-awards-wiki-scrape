import time
import random
from tqdm import tqdm
import os
import re
import requests
from bs4 import BeautifulSoup
from movie_extractor import extract_movie_details, get_soup, clean_text, extract_person_details
from nomination_extractor import process_category_page, extract_winner_info, extract_film_info, extract_person_info
from data_processor import AcademyAwardsDataProcessor
import pandas as pd
from datetime import datetime
from urllib.parse import urljoin

def extract_all_award_editions():
    """Extract all award editions and their details from Wikipedia"""
    print("Extracting all Academy Award editions...")
    
    # Start with a base list of all editions up to the most recent (97th)
    editions = []
    max_edition = 97  # Current max edition as of 2025
    start_year = 1929  # First Academy Awards
    
    # Generate editions from 1st to current
    for year in range(start_year, start_year + max_edition):
        edition_number = year - start_year + 1
        if edition_number <= max_edition:  # Only include up to the max edition
            editions.append({
                'edition': edition_number,
                'year': year,
                'venue_id': None,
                'cDate': None,
            })
    
    return editions

def extract_award_ceremony_details(edition_number):
    """Extract venue and other details from an Academy Award ceremony page"""
    # Handle special cases for URLs
    if edition_number == 1:
        url = "https://en.wikipedia.org/wiki/1st_Academy_Awards"
    elif edition_number == 2:
        url = "https://en.wikipedia.org/wiki/2nd_Academy_Awards"
    elif edition_number == 3:
        url = "https://en.wikipedia.org/wiki/3rd_Academy_Awards"
    elif edition_number in [11, 12, 13]:  # Special cases for 11th, 12th, 13th
        url = f"https://en.wikipedia.org/wiki/{edition_number}th_Academy_Awards"
    elif edition_number % 10 == 1 and edition_number % 100 != 11:  # Handle 21st, 31st, etc. but not 11th
        url = f"https://en.wikipedia.org/wiki/{edition_number}st_Academy_Awards"
    elif edition_number % 10 == 2 and edition_number % 100 != 12:  # Handle 22nd, 32nd, etc. but not 12th
        url = f"https://en.wikipedia.org/wiki/{edition_number}nd_Academy_Awards"
    elif edition_number % 10 == 3 and edition_number % 100 != 13:  # Handle 23rd, 33rd, etc. but not 13th
        url = f"https://en.wikipedia.org/wiki/{edition_number}rd_Academy_Awards"
    else:  # Handle 4th, 5th, etc. with "th" suffix
        url = f"https://en.wikipedia.org/wiki/{edition_number}th_Academy_Awards"
    
    print(f"Fetching ceremony details from {url}")
    soup = get_soup(url)
    
    if not soup:
        print(f"Failed to fetch {url}")
        return {
            'venue': None,
            'date': None,
            'hosts': [],
            'producers': [],
            'directors': [],
            'prehosts': []
        }
    
    details = {
        'venue': None,
        'date': None,
        'hosts': [],
        'producers': [],
        'directors': [],
        'prehosts': []
    }
    
    # Extract from infobox
    infobox = soup.find('table', class_='infobox')
    if infobox:
        rows = infobox.find_all('tr')
        
        for row in rows:
            header = row.find(['th'])
            if not header:
                continue
                
            header_text = header.get_text().strip().lower()
            cell = row.find('td')
            
            if not cell:
                continue
            
            # Venue
            if 'venue' in header_text or 'location' in header_text:
                venue_cell = cell
                venue_links = venue_cell.find_all('a')
                
                if venue_links:
                    # Prioritize links as they usually point to venue pages
                    venue_name = clean_text(venue_links[0].get_text())
                    details['venue'] = venue_name
                else:
                    # If no links, use the entire text
                    venue_text = clean_text(venue_cell.get_text())
                    details['venue'] = venue_text
            
            # Date
            if 'date' in header_text:
                date_text = cell.get_text().strip()
                # Extract date in various formats
                date_match = re.search(r'(\w+ \d+, \d{4})', date_text)
                if date_match:
                    details['date'] = date_match.group(1)
                else:
                    # Try another format
                    date_match = re.search(r'(\d+ \w+ \d{4})', date_text)
                    if date_match:
                        details['date'] = date_match.group(1)
            
            # Host(s)
            if 'host' in header_text and 'pre' not in header_text:
                host_links = cell.find_all('a')
                if host_links:
                    for link in host_links:
                        host_name = clean_text(link.get_text())
                        if host_name and not any(term in host_name.lower() for term in ['none', 'no host']):
                            details['hosts'].append(host_name)
                else:
                    host_text = cell.get_text().strip()
                    # Split by common separators
                    hosts = []
                    for separator in [', ', ' and ', '; ']:
                        if separator in host_text:
                            parts = host_text.split(separator)
                            hosts.extend([part.strip() for part in parts if part.strip()])
                            break
                    else:
                        # Single host
                        if host_text and not any(term in host_text.lower() for term in ['none', 'no host']):
                            hosts = [host_text]
                    
                    details['hosts'] = hosts
            
            # Pre-show hosts
            if 'pre' in header_text and 'host' in header_text:
                prehost_links = cell.find_all('a')
                if prehost_links:
                    for link in prehost_links:
                        prehost_name = clean_text(link.get_text())
                        if prehost_name and not any(term in prehost_name.lower() for term in ['none', 'no host']):
                            details['prehosts'].append(prehost_name)
                else:
                    prehost_text = cell.get_text().strip()
                    # Split by common separators
                    prehosts = []
                    for separator in [', ', ' and ', '; ']:
                        if separator in prehost_text:
                            parts = prehost_text.split(separator)
                            prehosts.extend([part.strip() for part in parts if part.strip()])
                            break
                    else:
                        # Single prehost
                        if prehost_text and not any(term in prehost_text.lower() for term in ['none', 'no host']):
                            prehosts = [prehost_text]
                    
                    details['prehosts'] = prehosts
            
            # Producer(s)
            if 'producer' in header_text:
                producer_links = cell.find_all('a')
                if producer_links:
                    for link in producer_links:
                        producer_name = clean_text(link.get_text())
                        if producer_name:
                            details['producers'].append(producer_name)
                else:
                    producer_text = cell.get_text().strip()
                    # Split by common separators
                    producers = []
                    for separator in [', ', ' and ', '; ']:
                        if separator in producer_text:
                            parts = producer_text.split(separator)
                            producers.extend([part.strip() for part in parts if part.strip()])
                            break
                    else:
                        # Single producer
                        if producer_text:
                            producers = [producer_text]
                    
                    details['producers'] = producers
            
            # Director(s)
            if 'director' in header_text:
                director_links = cell.find_all('a')
                if director_links:
                    for link in director_links:
                        director_name = clean_text(link.get_text())
                        if director_name:
                            details['directors'].append(director_name)
                else:
                    director_text = cell.get_text().strip()
                    # Split by common separators
                    directors = []
                    for separator in [', ', ' and ', '; ']:
                        if separator in director_text:
                            parts = director_text.split(separator)
                            directors.extend([part.strip() for part in parts if part.strip()])
                            break
                    else:
                        # Single director
                        if director_text:
                            directors = [director_text]
                    
                    details['directors'] = directors
    
    # If we still couldn't find venue info, try looking in the content
    if not details['venue']:
        # Try to find venue information in the first few paragraphs
        paragraphs = soup.find_all('p')[:5]  # Look at first 5 paragraphs
        
        for p in paragraphs:
            p_text = p.get_text().lower()
            if 'held at' in p_text or 'took place at' in p_text or 'venue' in p_text or 'location' in p_text:
                # Try to extract venue name from the paragraph
                venue_match = re.search(r'(held|took place|venue|location)(?:\s+was)?\s+at\s+(?:the\s+)?([^,.]+)', p_text)
                if venue_match:
                    venue_name = venue_match.group(2).strip().title()  # Convert to title case
                    details['venue'] = venue_name
    
    return details

def initialize_award_editions(processor):
    """Initialize all award editions and fetch their details"""
    print("Initializing award editions...")
    
    # Get base editions data
    editions = extract_all_award_editions()
    
    # Add editions to processor first
    for edition_data in editions:
        processor.add_award_edition(
            edition_data['edition'],
            edition_data['year'],
            venue_id=None,
            ceremony_date=None
        )
    
    # Now fetch details for each edition
    editions_to_process = sorted(list(set(processor.award_editions_df['edition'].tolist())))
    
    for edition in tqdm(editions_to_process, desc="Fetching ceremony details"):
        try:
            # Skip invalid editions
            if pd.isna(edition) or edition <= 0 or edition > 97:  # Only process up to 97th edition
                continue
                
            ceremony_details = extract_award_ceremony_details(int(edition))
            
            # Get award edition ID
            edition_id = processor.award_edition_lookup.get((edition, int(edition) + 1928))
            if not edition_id:
                continue
            
            # Update venue if found
            if ceremony_details['venue']:
                venue_name = ceremony_details['venue']
                
                # Add venue to database and get venue_id
                venue_id = processor.add_venue(venue_name)
                
                # Update award_edition with this venue
                processor.award_editions_df.loc[
                    processor.award_editions_df['award_edition_id'] == edition_id, 
                    'venue_id'
                ] = venue_id
                
                # If there's a date, update it
                if ceremony_details['date']:
                    processor.award_editions_df.loc[
                        processor.award_editions_df['award_edition_id'] == edition_id, 
                        'cDate'
                    ] = ceremony_details['date']
            
            # Add hosts as award_edition_person relationships
            for host_name in ceremony_details['hosts']:
                person_id = processor.add_person(host_name)
                if person_id:
                    # Get position ID for Host
                    position_id = processor.position_lookup.get('Host', 18)
                    # Add to award_edition_person
                    processor.add_award_edition_person(edition_id, person_id, position_id)
            
            # Add prehosts
            for prehost_name in ceremony_details['prehosts']:
                person_id = processor.add_person(prehost_name)
                if person_id:
                    # Get position ID for Pre-show Host (use existing or create new)
                    position_id = processor.position_lookup.get('Pre-show Host', 19)
                    # Add to award_edition_person
                    processor.add_award_edition_person(edition_id, person_id, position_id)
            
            # Add producers
            for producer_name in ceremony_details['producers']:
                person_id = processor.add_person(producer_name)
                if person_id:
                    # Get position ID for Producer (use existing or create new)
                    position_id = processor.position_lookup.get('Producer', 20)
                    # Add to award_edition_person
                    processor.add_award_edition_person(edition_id, person_id, position_id)
            
            # Add directors
            for director_name in ceremony_details['directors']:
                person_id = processor.add_person(director_name)
                if person_id:
                    # Get position ID for Director (use existing or create new)
                    position_id = processor.position_lookup.get('Director', 21)
                    # Add to award_edition_person
                    processor.add_award_edition_person(edition_id, person_id, position_id)
            
            # Be polite with crawling
            time.sleep(random.uniform(1, 2))
            
        except Exception as e:
            print(f"Error processing ceremony details for edition {edition}: {e}")
    
    print(f"Initialized {len(editions_to_process)} award editions")

def get_all_oscar_categories():
    """Get a list of all Academy Award categories from the categories page"""
    url = "https://en.wikipedia.org/wiki/Academy_Awards#Categories"
    soup = get_soup(url)
    
    if not soup:
        print("Failed to fetch Academy Award categories")
        return []
    
    # Find the categories section
    categories_section = None
    for section in soup.find_all('span', class_='mw-headline'):
        if 'Categories' in section.get_text():
            categories_section = section.parent
            break
    
    if not categories_section:
        print("Couldn't find categories section")
        return []
    
    # Extract categories from lists
    categories = []
    current_section = categories_section.find_next_sibling()
    
    while current_section and current_section.name != 'h2':
        if current_section.name == 'h3':
            # Found a subsection (e.g., "Current categories")
            next_section = current_section.find_next_sibling()
            if next_section and next_section.name in ['ul', 'ol']:
                for item in next_section.find_all('li'):
                    category_text = item.get_text().strip()
                    if 'Academy Award for Best' in category_text:
                        categories.append(category_text.replace('Academy Award for ', ''))
                    elif 'Best ' in category_text:
                        categories.append(category_text)
        
        current_section = current_section.find_next_sibling()
    
    # Clean up category names and format for URLs
    formatted_categories = []
    for category in categories:
        if 'Best ' in category:
            clean_category = category.replace('Best ', 'Best_')
            clean_category = re.sub(r'[^\w\s_]', '', clean_category)
            clean_category = clean_category.replace(' ', '_')
            formatted_categories.append(clean_category)
    
    return formatted_categories

def process_nominations_from_list_page(url, category_name):
    """Extract nominations from list pages that have different formats"""
    soup = get_soup(url)
    if not soup:
        print(f"Failed to fetch {url}")
        return []
    
    nominations = []
    tables = soup.find_all('table', class_='wikitable')
    
    for table in tables:
        rows = table.find_all('tr')
        if len(rows) <= 1:  # Skip tables with just a header
            continue
        
        # Check table headers to understand structure
        header_row = rows[0]
        headers = [th.get_text().strip().lower() for th in header_row.find_all(['th', 'td'])]
        
        year_idx = None
        film_idx = None
        winners_idx = None
        person_idx = None
        
        # Try to identify column positions
        for idx, header in enumerate(headers):
            if any(term in header for term in ['year', 'ceremony', 'oscar']):
                year_idx = idx
            elif any(term in header for term in ['film', 'movie', 'picture', 'work', 'title']):
                film_idx = idx
            elif 'winner' in header:
                winners_idx = idx
            elif any(term in header for term in ['actor', 'actress', 'director', 'person', 'nominee']):
                person_idx = idx
        
        # If we couldn't identify essential columns, try a default layout
        if year_idx is None:
            year_idx = 0  # Assume first column is year
        if film_idx is None:
            # For Best International Feature, the film might be in column 2 or 3
            if category_name == 'Best_International_Feature_Film':
                if len(headers) > 2:
                    film_idx = 2  # Often title is in 3rd column
                else:
                    film_idx = 1
            else:
                film_idx = 1 if len(headers) > 1 else 0  # Assume second column is film
        
        # Process each row
        current_year = None
        nominee_count = 0
        
        for row in rows[1:]:  # Skip header row
            cells = row.find_all(['td', 'th'])
            if len(cells) <= max(year_idx, film_idx):
                continue
            
            # Check for year - sometimes the year is in a separate row or a rowspan
            year_cell = cells[year_idx]
            year_text = year_cell.get_text().strip()
            year_match = re.search(r'(\d{4})', year_text)
            
            if year_match:
                current_year = int(year_match.group(1))
                nominee_count = 0  # Reset for new year
            elif current_year is None:
                continue  # Skip if we don't have a year yet
            
            year = current_year
            edition = year - 1928  # Approximate edition
            
            # Extract film
            film_cell = cells[film_idx]
            film_title, film_url = extract_film_info(film_cell)
            
            # For International Film, the title might include country in parentheses
            if category_name == 'Best_International_Feature_Film' and film_title:
                # Extract country if in parentheses
                country_match = re.search(r'\((.*?)\)', film_title)
                country = country_match.group(1) if country_match else None
                
                # Clean up the title
                if country_match:
                    film_title = film_title.replace(country_match.group(0), '').strip()
            
            # Determine if winner
            # First nominee for each year is typically the winner unless explicitly marked
            is_winner = False
            
            # Increment nominee count for this year
            nominee_count += 1
            
            # First nominee is usually the winner
            if nominee_count == 1:
                is_winner = True
            
            # But check for explicit winner markers
            if winners_idx is not None and winners_idx < len(cells):
                winner_cell = cells[winners_idx]
                winner_text = winner_cell.get_text().strip().lower()
                is_winner = 'yes' in winner_text or 'won' in winner_text
            else:
                # Check if row is marked as winner (bold, special class, etc.)
                explicit_winner = extract_winner_info(row)
                if explicit_winner:
                    is_winner = True
            
            # Extract people associated with this nomination
            people = []
            # If we have a dedicated person column
            if person_idx is not None and person_idx < len(cells):
                person_cell = cells[person_idx]
                extracted_people = extract_person_info(person_cell)
                if extracted_people:
                    people = extracted_people
            else:
                # Try to find people in all cells except film and year
                for idx, cell in enumerate(cells):
                    if idx != film_idx and idx != year_idx:
                        people.extend(extract_person_info(cell))
            
            # If still no people found, add a placeholder
            if not people and category_name:
                # For some categories, we can make an educated guess about the role
                placeholder_position = None
                if 'actor' in category_name.lower():
                    placeholder_position = "Actor"
                elif 'actress' in category_name.lower():
                    placeholder_position = "Actress"
                elif 'director' in category_name.lower():
                    placeholder_position = "Director"
                
                # We could still have the person name in the film cell in some cases
                if placeholder_position and film_title:
                    # But don't treat film titles as person names
                    if not any(generic in film_title.lower() for generic in ['academy award', 'oscar', 'nominees']):
                        people = [(film_title, film_url)]
            
            nomination = {
                'category': category_name,
                'film_title': film_title,
                'film_url': film_url,
                'people': people,
                'is_winner': is_winner,
                'edition': edition,
                'ceremony_year': year
            }
            
            nominations.append(nomination)
    
    print(f"Extracted {len(nominations)} nominations from list page for {category_name}")
    return nominations

def main():
    # Start the timer for overall execution
    start_time = time.time()
    
    print("Academy Awards Web Scraper")
    print("-------------------------")
    
    # Create data directory
    os.makedirs('data', exist_ok=True)
    
    # Initialize data processor
    processor = AcademyAwardsDataProcessor()
    
    # Initialize categories with just their names
    print("Initializing categories...")
    categories = [
        'Best Picture',
        'Best Director',
        'Best Actor',
        'Best Actress',
        'Best Supporting Actor',
        'Best Supporting Actress',
        'Best Original Screenplay',
        'Best Adapted Screenplay',
        'Best Animated Feature',
        'Best International Feature Film',
        'Best Documentary Feature',
        'Best Original Score',
        'Best Original Song',
        'Best Cinematography',
        'Best Film Editing',
        'Best Production Design',
        'Best Costume Design',
        'Best Makeup and Hairstyling',
        'Best Sound',
        'Best Visual Effects'
    ]
    
    # Add categories to processor
    for category_name in categories:
        processor.add_category(category_name)
    
    # All categories to scrape (with underscores for URL formatting)
    all_categories = [category.replace(' ', '_') for category in categories]
    
    # Print all categories we'll scrape
    print(f"Found {len(all_categories)} categories to scrape:")
    for category in all_categories:
        print(f"- {category.replace('_', ' ')}")
    
    # Initialize award editions FIRST (as requested)
    initialize_award_editions(processor)
    
    # Basic categories to use the list pages as backup or primary source
    LIST_PAGES = {
        'Best_Picture': 'https://en.wikipedia.org/wiki/List_of_Academy_Award_winners_and_nominees_for_Best_Picture',
        'Best_Actor': 'https://en.wikipedia.org/wiki/List_of_Academy_Award_winners_and_nominees_for_Best_Actor',
        'Best_Actress': 'https://en.wikipedia.org/wiki/List_of_Academy_Award_winners_and_nominees_for_Best_Actress',
        'Best_Supporting_Actor': 'https://en.wikipedia.org/wiki/List_of_Academy_Award_winners_and_nominees_for_Best_Supporting_Actor',
        'Best_Supporting_Actress': 'https://en.wikipedia.org/wiki/List_of_Academy_Award_winners_and_nominees_for_Best_Supporting_Actress',
        'Best_Director': 'https://en.wikipedia.org/wiki/List_of_Academy_Award_winners_and_nominees_for_Best_Director',
        'Best_International_Feature_Film': 'https://en.wikipedia.org/wiki/List_of_Academy_Award_winners_and_nominees_for_Best_International_Feature_Film',
        'Best_Original_Screenplay': 'https://en.wikipedia.org/wiki/List_of_Academy_Award_winners_and_nominees_for_Best_Original_Screenplay',
        'Best_Adapted_Screenplay': 'https://en.wikipedia.org/wiki/List_of_Academy_Award_winners_and_nominees_for_Best_Adapted_Screenplay',
        'Best_Animated_Feature': 'https://en.wikipedia.org/wiki/List_of_Academy_Award_winners_and_nominees_for_Best_Animated_Feature',
        'Best_Documentary_Feature': 'https://en.wikipedia.org/wiki/List_of_Academy_Award_winners_and_nominees_for_Best_Documentary_Feature',
        'Best_Original_Score': 'https://en.wikipedia.org/wiki/List_of_Academy_Award_winners_and_nominees_for_Best_Original_Score',
        'Best_Original_Song': 'https://en.wikipedia.org/wiki/List_of_Academy_Award_winners_and_nominees_for_Best_Original_Song',
        'Best_Cinematography': 'https://en.wikipedia.org/wiki/List_of_Academy_Award_winners_and_nominees_for_Best_Cinematography',
        'Best_Film_Editing': 'https://en.wikipedia.org/wiki/List_of_Academy_Award_winners_and_nominees_for_Best_Film_Editing',
        'Best_Production_Design': 'https://en.wikipedia.org/wiki/List_of_Academy_Award_winners_and_nominees_for_Best_Production_Design',
        'Best_Costume_Design': 'https://en.wikipedia.org/wiki/List_of_Academy_Award_winners_and_nominees_for_Best_Costume_Design',
        'Best_Makeup_and_Hairstyling': 'https://en.wikipedia.org/wiki/List_of_Academy_Award_winners_and_nominees_for_Best_Makeup_and_Hairstyling',
        'Best_Sound': 'https://en.wikipedia.org/wiki/List_of_Academy_Award_winners_and_nominees_for_Best_Sound',
        'Best_Visual_Effects': 'https://en.wikipedia.org/wiki/List_of_Academy_Award_winners_and_nominees_for_Best_Visual_Effects',
    }
    
    # Base URL pattern for consistent Wikipedia access
    BASE_URL = 'https://en.wikipedia.org/wiki/Academy_Award_for_'
    
    # Alternative URLs for special cases
    ALTERNATE_URLS = {
        'Best_International_Feature_Film': BASE_URL + 'Best_Foreign_Language_Film',
        'Best_Sound': BASE_URL + 'Best_Sound_Mixing',
        'Best_Production_Design': BASE_URL + 'Best_Art_Direction',
    }
    
    # Count total nominations processed
    total_nominations = 0
    category_times = {}
    category_nomination_counts = {}
    movie_details_lookup = {}  # Cache movie details to avoid duplicate requests
    
    # Process each award category
    for award in tqdm(all_categories, desc="Processing category pages"):
        # Track time for this category
        category_start_time = time.time()
        
        print(f"\nProcessing category: {award}")
        
        # For International Feature Film, go directly to the list page as instructed
        if award == 'Best_International_Feature_Film':
            list_url = LIST_PAGES[award]
            print(f"Using list page for International Film: {list_url}")
            nominations = process_nominations_from_list_page(list_url, award)
        else:
            # Try multiple URL patterns in sequence
            nominations = []
            
            # First try the category page with winner and nominees section
            if award in ALTERNATE_URLS:
                category_url = f"{ALTERNATE_URLS[award]}#Winners_and_nominees"
            else:
                category_url = f"{BASE_URL}{award}#Winners_and_nominees"
            
            print(f"Trying URL: {category_url}")
            nominations = process_category_page(award, category_url)
            
            # If no nominations found, try without the anchor
            if len(nominations) == 0:
                if award in ALTERNATE_URLS:
                    alt_url = ALTERNATE_URLS[award]
                else:
                    alt_url = f"{BASE_URL}{award}"
                
                print(f"Trying without anchor: {alt_url}")
                nominations = process_category_page(award, alt_url)
            
            # If still no nominations, try list page as third option
            if len(nominations) == 0 and award in LIST_PAGES:
                list_url = LIST_PAGES[award]
                print(f"Trying list page: {list_url}")
                nominations = process_nominations_from_list_page(list_url, award)
            
            # Final fallback: try generic list pattern
            if len(nominations) == 0:
                fallback_url = f"https://en.wikipedia.org/wiki/List_of_Academy_Award_winners_and_nominees_for_{award}"
                print(f"Trying fallback URL: {fallback_url}")
                nominations = process_nominations_from_list_page(fallback_url, award)
        
        category_nomination_counts[award] = len(nominations)
        print(f"Found {len(nominations)} nominations for {award}")
        total_nominations += len(nominations)
        
        # Process each nomination
        for nomination in tqdm(nominations, desc=f"Processing {award} nominations", leave=False):
            # Get full information for every movie
            movie_details = None
            movie_url = nomination.get('film_url')
            
            if movie_url:
                try:
                    # Get full movie details for every film
                    movie_details = extract_movie_details(movie_url)
                    # Be polite with crawling
                    time.sleep(random.uniform(0.5, 1.5))
                except Exception as e:
                    print(f"Error fetching movie details for {nomination.get('film_title')}: {e}")
            
            # Process the nomination with all available details
            processor.process_nomination(nomination, movie_details)
            
            # Process person information from the nomination
            for person_name, person_url in nomination.get('people', []):
                if person_url:
                    try:
                        # Get person details
                        person_details = extract_person_details(person_url)
                        # Set the full_name to person_name if it's not available from the Wikipedia page
                        if not person_details.get('full_name'):
                            person_details['full_name'] = person_name
                        if person_details:
                            # Update person information
                            processor.update_person_info(person_name, person_details)
                        # Be polite with crawling
                        time.sleep(random.uniform(0.5, 1.0))
                    except Exception as e:
                        print(f"Error fetching person details for {person_name}: {e}")
                        # Use basic info with the name we already have
                        basic_details = {'full_name': person_name}
                        processor.update_person_info(person_name, basic_details)
        
        # Be polite with crawling between categories
        time.sleep(random.uniform(2, 3))
        
        # Record time for this category
        category_end_time = time.time()
        category_times[award] = category_end_time - category_start_time
    
    # Save all data to CSV files
    processor.save_to_csv()
    
    # Print statistics
    processor.print_stats()
    
    # Calculate and print timing information
    end_time = time.time()
    total_time = end_time - start_time
    
    print(f"\nTotal nominations processed: {total_nominations}")
    print(f"Total scraping time: {total_time:.2f} seconds ({total_time/60:.2f} minutes)")
    
    # Print category-specific timing and nomination counts
    print("\nNominations by category:")
    for category, count in sorted(category_nomination_counts.items(), key=lambda x: x[1], reverse=True):
        print(f"- {category.replace('_', ' ')}: {count} nominations")
    
    print("\nTime spent per category:")
    for category, category_time in sorted(category_times.items(), key=lambda x: x[1], reverse=True):
        print(f"- {category.replace('_', ' ')}: {category_time:.2f} seconds ({category_time/60:.2f} minutes)")
    
    print("\nScraping completed successfully!")

if __name__ == "__main__":
    main() 