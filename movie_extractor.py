import re
import time
import random
from bs4 import BeautifulSoup
import requests
from datetime import datetime
from urllib.parse import urljoin

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

def get_soup(url, max_retries=5):
    """Get BeautifulSoup object from URL with retries and random delays"""
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
    
    # Convert relative URLs to absolute Wikipedia URLs
    if url and not url.startswith(('http://', 'https://')):
        url = f"https://en.wikipedia.org{url}"
    
    for attempt in range(max_retries):
        try:
            time.sleep(random.uniform(1, 3))  # Random delay to be polite
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return BeautifulSoup(response.text, 'html.parser')
        except Exception as e:
            print(f"Error fetching {url}: {e}. Attempt {attempt+1}/{max_retries}")
            time.sleep(2 * attempt)  # Exponential backoff
    
    print(f"Failed to fetch {url} after {max_retries} attempts")
    return None

def extract_movie_details(url):
    """Extract movie details from Wikipedia page"""
    soup = get_soup(url)
    
    if not soup:
        return None
    
    # Initialize result dictionary
    movie_data = {
        'title': None,
        'release_dates': [],
        'runtime': None,
        'languages': [],
        'countries': [],
        'directors': [],
        'producers': [],
        'writers': [],
        'editors': [],
        'cinematographers': [],
        'composers': [],
        'production_companies': []
    }
    
    # Extract title from page heading
    title_elem = soup.find('h1', id='firstHeading')
    if title_elem:
        movie_data['title'] = clean_text(title_elem.get_text())
    
    # Find infobox
    infobox = soup.find('table', {'class': 'infobox'})
    if not infobox:
        return movie_data
    
    # Extract data from infobox
    for row in infobox.find_all('tr'):
        header = row.find('th')
        if not header:
            continue
        
        header_text = header.get_text().strip().lower()
        value_cell = row.find('td')
        
        if not value_cell:
            continue
        
        # Runtime
        if 'running time' in header_text:
            runtime_text = value_cell.get_text().strip()
            # Try to extract minutes
            minutes_match = re.search(r'(\d+)\s*(?:min|minutes)', runtime_text)
            if minutes_match:
                movie_data['runtime'] = int(minutes_match.group(1))
        
        # Release date
        elif 'release date' in header_text:
            # Get all dates
            date_links = value_cell.find_all('a')
            
            if date_links:
                for link in date_links:
                    date_text = link.get_text().strip()
                    # Check if this looks like a date
                    if re.search(r'\d{1,2}\s+\w+\s+\d{4}|\w+\s+\d{1,2},\s+\d{4}|\d{4}-\d{2}-\d{2}', date_text):
                        movie_data['release_dates'].append(date_text)
            
            # If no links with dates, try to extract directly
            if not movie_data['release_dates']:
                date_text = value_cell.get_text().strip()
                # Split by common date separators
                date_parts = re.split(r'[,;()]', date_text)
                for part in date_parts:
                    part = part.strip()
                    if re.search(r'\d{4}', part):  # If part contains a year
                        movie_data['release_dates'].append(part)
        
        # Languages
        elif 'language' in header_text:
            language_links = value_cell.find_all('a')
            
            if language_links:
                movie_data['languages'] = [clean_text(link.get_text()) for link in language_links if not link.get('href', '').startswith('#')]
            else:
                # If no links, extract from text
                languages_text = value_cell.get_text().strip()
                # Split by common separators
                languages = []
                for sep in [',', ';', 'and']:
                    if sep in languages_text:
                        languages = [lang.strip() for lang in languages_text.split(sep) if lang.strip()]
                        break
                
                if languages:
                    movie_data['languages'] = languages
                else:
                    movie_data['languages'] = [languages_text]
        
        # Countries
        elif 'country' in header_text:
            country_links = value_cell.find_all('a')
            
            if country_links:
                movie_data['countries'] = [clean_text(link.get_text()) for link in country_links if not link.get('href', '').startswith('#')]
            else:
                # If no links, extract from text
                countries_text = value_cell.get_text().strip()
                # Split by common separators
                countries = []
                for sep in [',', ';', 'and']:
                    if sep in countries_text:
                        countries = [country.strip() for country in countries_text.split(sep) if country.strip()]
                        break
                
                if countries:
                    movie_data['countries'] = countries
                else:
                    movie_data['countries'] = [countries_text]
        
        # Director
        elif 'direct' in header_text:
            director_links = value_cell.find_all('a')
            
            if director_links:
                directors = []
                for link in director_links:
                    # Skip footnote links
                    if link.get('href', '').startswith('#'):
                        continue
                    
                    name = clean_text(link.get_text())
                    if name:
                        directors.append((name, urljoin('https://en.wikipedia.org', link.get('href', ''))))
                
                if directors:
                    movie_data['directors'] = directors
            else:
                # Try to extract directly
                director_text = value_cell.get_text().strip()
                # Split by common name separators
                for sep in [',', ';', 'and']:
                    if sep in director_text:
                        names = [name.strip() for name in director_text.split(sep) if name.strip()]
                        movie_data['directors'] = [(name, None) for name in names]
                        break
                
                if not movie_data['directors']:
                    movie_data['directors'] = [(director_text, None)]
        
        # Producers
        elif 'produc' in header_text and 'company' not in header_text:
            producer_links = value_cell.find_all('a')
            
            if producer_links:
                producers = []
                for link in producer_links:
                    # Skip footnote links
                    if link.get('href', '').startswith('#'):
                        continue
                    
                    name = clean_text(link.get_text())
                    if name:
                        producers.append((name, urljoin('https://en.wikipedia.org', link.get('href', ''))))
                
                if producers:
                    movie_data['producers'] = producers
            else:
                # Try to extract directly
                producer_text = value_cell.get_text().strip()
                # Split by common name separators
                for sep in [',', ';', 'and']:
                    if sep in producer_text:
                        names = [name.strip() for name in producer_text.split(sep) if name.strip()]
                        movie_data['producers'] = [(name, None) for name in names]
                        break
                
                if not movie_data['producers']:
                    movie_data['producers'] = [(producer_text, None)]
    
    # Extract production companies using the dedicated function
    movie_data['production_companies'] = extract_production_companies(infobox)
    
    # Extract additional crew members from infobox
    # Writers
    writer_row = find_infobox_row(infobox, 'Writer') or find_infobox_row(infobox, 'Written by') or find_infobox_row(infobox, 'Screenplay by')
    if writer_row:
        writers = extract_person_links(writer_row)
        if writers:
            movie_data['writers'] = writers
    
    # Editors
    editor_row = find_infobox_row(infobox, 'Editor') or find_infobox_row(infobox, 'Edited by') or find_infobox_row(infobox, 'Film editor')
    if editor_row:
        editors = extract_person_links(editor_row)
        if editors:
            movie_data['editors'] = editors
    
    # Cinematographers
    cinematographer_row = find_infobox_row(infobox, 'Cinematography') or find_infobox_row(infobox, 'Cinematographer')
    if cinematographer_row:
        cinematographers = extract_person_links(cinematographer_row)
        if cinematographers:
            movie_data['cinematographers'] = cinematographers
    
    # Composers
    composer_row = find_infobox_row(infobox, 'Music by') or find_infobox_row(infobox, 'Composer')
    if composer_row:
        composers = extract_person_links(composer_row)
        if composers:
            movie_data['composers'] = composers
    
    return movie_data

def extract_person_details(person_url):
    """Extract details from a person's Wikipedia page"""
    if not person_url:
        return {}
        
    # Convert relative URLs to absolute Wikipedia URLs
    if not person_url.startswith(('http://', 'https://')):
        person_url = f"https://en.wikipedia.org{person_url}"
    
    try:
        soup = get_soup(person_url)
        if not soup:
            return {}
        
        details = {
            'full_name': None,
            'birth_date': None,
            'death_date': None,
            'country': None,
            'gender': None
        }
        
        # Extract name
        title_elem = soup.find('h1', id='firstHeading')
        if title_elem:
            details['full_name'] = clean_text(title_elem.text)
        
        # Extract from infobox
        infobox = soup.find('table', class_='infobox')
        if infobox:
            # Birth date
            birth_date_elem = infobox.find('span', class_='bday')
            if birth_date_elem:
                details['birth_date'] = birth_date_elem.text.strip()
            else:
                # Try alternative method for birth date
                for row in infobox.find_all('tr'):
                    header = row.find('th')
                    if header and 'born' in header.text.lower():
                        cell = row.find('td')
                        if cell:
                            date_match = re.search(r'(\d{1,2}\s+\w+\s+\d{4}|\w+\s+\d{1,2},\s+\d{4})', cell.text)
                            if date_match:
                                details['birth_date'] = date_match.group(1)
            
            # Death date
            death_date_elem = infobox.find('span', class_='dday')
            if death_date_elem:
                details['death_date'] = death_date_elem.text.strip()
            else:
                # Try alternative method for death date
                for row in infobox.find_all('tr'):
                    header = row.find('th')
                    if header and 'died' in header.text.lower():
                        cell = row.find('td')
                        if cell:
                            date_match = re.search(r'(\d{1,2}\s+\w+\s+\d{4}|\w+\s+\d{1,2},\s+\d{4})', cell.text)
                            if date_match:
                                details['death_date'] = date_match.group(1)
            
            # Gender
            for row in infobox.find_all('tr'):
                header = row.find('th')
                if header and 'gender' in header.text.lower():
                    cell = row.find('td')
                    if cell:
                        gender_text = cell.text.strip().lower()
                        if 'male' in gender_text and 'female' not in gender_text:
                            details['gender'] = 'Male'
                        elif 'female' in gender_text:
                            details['gender'] = 'Female'
            
            # If gender not found, try to infer from pronouns in the first paragraph
            if not details['gender']:
                first_paragraph = soup.select_one('#mw-content-text p')
                if first_paragraph:
                    paragraph_text = first_paragraph.text.lower()
                    he_count = paragraph_text.count(' he ') + paragraph_text.count(' his ') + paragraph_text.count(' him ')
                    she_count = paragraph_text.count(' she ') + paragraph_text.count(' her ') + paragraph_text.count(' hers ')
                    if he_count > she_count:
                        details['gender'] = 'Male'
                    elif she_count > he_count:
                        details['gender'] = 'Female'
            
            # Country/Nationality
            rows = infobox.find_all('tr')
            for row in rows:
                header = row.find('th')
                if header and ('born' in header.text.lower() or 'nationality' in header.text.lower()):
                    cell = row.find('td')
                    if cell:
                        country_match = re.search(r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\b', cell.text)
                        if country_match:
                            details['country'] = country_match.group(1)
        
        return details
    except Exception as e:
        print(f"Error in extract_person_details: {e}")
        # Return a partial details dictionary with whatever we have
        return {'full_name': None, 'birth_date': None, 'death_date': None, 'country': None, 'gender': None}

def parse_person_name(name):
    """Parse person name into first, middle, and last names"""
    if not name:
        return None, None, None
    
    # Remove parentheses content
    name = re.sub(r'\([^)]*\)', '', name).strip()
    
    # Handle empty names after cleaning
    if not name:
        return None, None, None
    
    # Split name by spaces
    parts = name.split()
    
    # Handle cases where there are no parts after splitting
    if not parts:
        return None, None, None
    
    if len(parts) == 1:
        return parts[0], None, ""
    elif len(parts) == 2:
        return parts[0], None, parts[1]
    else:
        return parts[0], ' '.join(parts[1:-1]), parts[-1]

def extract_production_companies(infobox):
    """Extract production companies from infobox"""
    companies = []
    
    # Try different labels for production company
    for label in ['Production company', 'Production companies', 'Studio', 'Studios']:
        row = find_infobox_row(infobox, label)
        if row:
            # Get all links in the cell, as these are usually the company names
            links = row.find_all('a')
            
            if links:
                for link in links:
                    # Skip links to Wikipedia templates or footnotes
                    if 'Template:' in link.get('href', '') or link.get('href', '').startswith('#'):
                        continue
                        
                    company_name = clean_text(link.get_text())
                    
                    # Skip footnote references and empty names
                    if company_name and not company_name.startswith('[') and len(company_name) > 1:
                        companies.append(company_name)
            else:
                # If no links found, use plaintext
                cell = row.find('td')
                if cell:
                    # Remove HTML tags and references
                    text = re.sub(r'\[\d+\]', '', cell.get_text().strip())
                    text = re.sub(r'\[[a-z]\]', '', text)  # Remove [b], [c], etc.
                    
                    # Split by common separators
                    parts = []
                    for sep in [', ', ' and ', '\n']:
                        if sep in text:
                            parts = [p.strip() for p in text.split(sep) if p.strip()]
                            break
                    
                    if not parts:
                        parts = [text]
                    
                    for part in parts:
                        if part and not part.startswith('[') and len(part) > 1:
                            companies.append(clean_text(part))
    
    return list(set(companies))  # Remove duplicates

def find_infobox_row(infobox, label):
    """Find a row in an infobox with the given label"""
    if not infobox:
        return None
        
    for row in infobox.find_all('tr'):
        header = row.find('th')
        if header and label.lower() in header.get_text().strip().lower():
            return row
    
    return None

def extract_person_links(row):
    """Extract person names and links from a table row"""
    if not row:
        return []
        
    cell = row.find('td')
    if not cell:
        return []
        
    links = cell.find_all('a')
    people = []
    
    if links:
        for link in links:
            # Skip footnote links and non-person links
            if link.get('href', '').startswith('#') or 'Template:' in link.get('href', ''):
                continue
                
            name = clean_text(link.get_text())
            if name and len(name) > 2:  # Ensure we have a valid name
                people.append((name, urljoin('https://en.wikipedia.org', link.get('href', ''))))
    else:
        # If no links, try text extraction
        text = cell.get_text().strip()
        text = re.sub(r'\[\d+\]', '', text)  # Remove footnote references
        
        # Split by common separators
        names = []
        for sep in [',', ';', ' and ']:
            if sep in text:
                names = [name.strip() for name in text.split(sep) if name.strip()]
                break
                
        if names:
            people = [(name, None) for name in names]
        elif text:
            people = [(text, None)]
    
    return people 