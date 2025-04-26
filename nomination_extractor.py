import re
import time
import random
from bs4 import BeautifulSoup
from movie_extractor import get_soup, clean_text, extract_year

def extract_award_edition_info(header_text):
    """Extract edition number and year from header text"""
    # Try common formats for Academy Award headers
    patterns = [
        r'(\d{1,2})(st|nd|rd|th) Academy Awards \((\d{4})\)',  # "81st Academy Awards (2009)"
        r'(\d{4}) \((\d{1,2})(st|nd|rd|th)\)',                 # "2009 (81st)"
        r'Academy Awards, (\d{4})',                            # "Academy Awards, 2009"
        r'(\d{1,2})(st|nd|rd|th) Academy Awards',              # "81st Academy Awards"
        r'Ceremony (\d{1,2})(st|nd|rd|th) \((\d{4})\)',        # "Ceremony 81st (2009)" 
        r'Ceremony (\d{1,2})(st|nd|rd|th)',                    # "Ceremony 81st"
        r'(\d{1,2})(st|nd|rd|th) Academy Award',               # "81st Academy Award"
    ]
    
    for pattern in patterns:
        match = re.search(pattern, header_text)
        if match:
            groups = match.groups()
            if len(groups) == 3:  # Pattern with both edition and year
                edition = int(groups[0])
                ceremony_year = int(groups[2])
                return edition, ceremony_year
            elif len(groups) == 2 and groups[0].isdigit() and len(groups[0]) == 4:  # Year first
                ceremony_year = int(groups[0])
                edition_str = groups[1]
                edition = int(re.search(r'(\d+)', edition_str).group(1))
                return edition, ceremony_year
            elif len(groups) == 2:  # Just edition
                edition = int(groups[0])
                # Make a guess at year based on edition number
                # First Academy Awards was in 1929
                ceremony_year = 1928 + edition
                return edition, ceremony_year
            elif len(groups) == 1 and len(groups[0]) == 4 and groups[0].isdigit():  # Just year
                ceremony_year = int(groups[0])
                # Make a guess at edition number
                edition = ceremony_year - 1928
                return edition, ceremony_year
    
    # If no specific patterns match, just look for any year
    year = extract_year(header_text)
    if year:
        year_int = int(year)
        if 1927 <= year_int <= 2024:  # Sanity check for reasonable Academy Award years
            # Make a guess at edition number
            edition = year_int - 1928
            return edition, year_int
    
    return None, None

def get_header_for_table(table):
    """Find the header text for a table"""
    # First check for a caption
    caption = table.find('caption')
    if caption:
        return caption.get_text().strip()
    
    # Try to find a header before the table
    element = table.find_previous(['h2', 'h3', 'h4', 'h5', 'h6'])
    if element:
        return element.get_text().strip()
    
    return ""

def identify_table_type(table):
    """Identify what type of award table this is"""
    # Check table headers or caption for clues
    caption = table.find('caption')
    caption_text = caption.get_text().lower() if caption else ""
    
    # Check table headers
    headers = [th.get_text().strip().lower() for th in table.find_all('th')]
    
    if any('director' in h for h in headers) or 'director' in caption_text:
        return 'director'
    elif any('actor' in h for h in headers) or 'actor' in caption_text:
        return 'actor'
    elif any('actress' in h for h in headers) or 'actress' in caption_text:
        return 'actress'
    elif any('film' in h for h in headers) or any('movie' in h for h in headers) or 'film' in caption_text:
        return 'film'
    else:
        # Default to film if no clear pattern
        return 'film'

def extract_winner_info(row, table_type='film'):
    """Determine if a row represents a winner, and extract relevant info"""
    # Common winner indicators in Wikipedia tables
    # Yellow background is the most reliable indicator for Oscar winners on Wikipedia
    has_winner_bg = 'background:#FAEB86' in str(row) or 'background-color:#FAEB86' in str(row) or 'bgcolor="#FAEB86"' in str(row)
    
    # Clear textual markers
    winner_text = row.get_text().lower()
    has_winner_text = False
    
    # Check for explicit winner text in specific columns, not anywhere in the row
    for cell in row.find_all(['td', 'th']):
        cell_text = cell.get_text().strip().lower()
        if cell_text == 'yes' or cell_text == 'won' or cell_text == 'win' or cell_text == 'winner':
            has_winner_text = True
            break
    
    # Look for ticks or check marks which often indicate winners
    has_tick = '✓' in winner_text or '✔' in winner_text or '☑' in winner_text
    
    # Winner class is also a reliable indicator
    has_winner_class = 'class="yes"' in str(row) or 'class="winner"' in str(row)
    
    # Bold/italic alone is too broad and catches too many false positives
    # Only count it if combined with other indicators or in specific contexts
    has_bold = bool(row.find(['b', 'strong']))
    has_italic = bool(row.find(['i', 'em']))
    has_bold_italic = has_bold and has_italic
    
    # Primary winner indicators - these are highly reliable
    primary_indicators = has_winner_bg or has_winner_text or has_tick or has_winner_class
    
    # Secondary indicators - only use these if they reinforce primary indicators
    secondary_indicators = has_bold_italic
    
    # Be more conservative in determining winners
    is_winner = primary_indicators or (secondary_indicators and 'win' in winner_text)
    
    return is_winner

def extract_film_info(cell):
    """Extract film title and URL from a table cell"""
    # First look for links - these are most reliable
    film_links = cell.find_all('a')
    
    for link in film_links:
        link_text = clean_text(link.get_text())
        link_url = link.get('href', '')
        
        # Skip non-film links like references or years
        if link_text and len(link_text) > 1 and not link_text.isdigit() and 'cite' not in link_url and not link_text.startswith('#'):
            # Common patterns for film URLs in Wikipedia
            if ('film' in link_url.lower() or 
                'movie' in link_url.lower() or 
                re.search(r'_\(\d{4}_film\)', link_url) or
                not any(term in link_url.lower() for term in ['category:', 'wikipedia:', 'help:', 'file:'])):
                return link_text, link_url
    
    # If no obvious film link, take the first reasonable link
    if film_links:
        for link in film_links:
            link_text = clean_text(link.get_text())
            if link_text and len(link_text) > 1 and not link_text.isdigit() and not link_text.startswith('#'):
                return link_text, link.get('href', '')
    
    # If still no link, extract from text
    film_title = clean_text(cell.get_text())
    
    # Try to clean up the title
    film_title = re.sub(r'\[\d+\]', '', film_title)  # Remove reference markers
    film_title = re.sub(r'\([^)]*\)', '', film_title).strip()  # Remove parentheses content
    
    return film_title, None

def extract_person_info(cell):
    """Extract person information from a cell"""
    if not cell:
        return []
        
    people = []
    
    # First try to find all links, which are usually people
    person_links = cell.find_all('a')
    if person_links:
        for link in person_links:
            # Skip links that are footnotes or citations
            if link.get('href', '').startswith('#') or 'cite_note' in link.get('href', ''):
                continue
                
            # Skip links to films, categories, etc.
            skip_terms = ['film', 'movie', 'category:', 'index.php', 'template:', 'file:']
            if any(term in link.get('href', '').lower() for term in skip_terms):
                continue
                
            person_name = clean_text(link.get_text())
            
            # Skip empty names or too short names
            if not person_name or len(person_name) < 3:
                continue
                
            # Skip if name contains non-person terms
            if any(term in person_name.lower() for term in ['film', 'movie', 'category', 'award', 'oscar']):
                continue
                
            # Get the link URL
            person_url = link.get('href', '')
            if person_url and not person_url.startswith(('http://', 'https://')):
                person_url = f"https://en.wikipedia.org{person_url}"
                
            people.append((person_name, person_url))
    else:
        # If no links found, try to extract from text
        text = cell.get_text().strip()
        
        # Remove footnotes and formatting
        text = re.sub(r'\[\d+\]', '', text)
        text = re.sub(r'\([^)]*\)', '', text)
        
        # Try to split by common separators
        parts = []
        for sep in [',', ';', ' and ', '\n']:
            if sep in text:
                parts = [part.strip() for part in text.split(sep) if part.strip()]
                break
                
        if parts:
            for part in parts:
                # Skip very short parts or those with numbers (likely not names)
                if len(part) < 3 or any(c.isdigit() for c in part):
                    continue
                    
                # Skip parts that are likely not names
                if any(term in part.lower() for term in ['film', 'movie', 'award', 'oscar', 'nominee']):
                    continue
                    
                people.append((part, None))
        elif text and len(text) > 3:
            # Single name
            people.append((text, None))
            
    return people

def find_film_and_person_columns(table):
    """Find the column indices for film and person information"""
    film_col_idx = None
    person_col_idx = None
    
    # First check headers
    header_row = table.find('tr')
    if header_row:
        headers = header_row.find_all(['th', 'td'])
        
        for idx, header in enumerate(headers):
            header_text = header.get_text().strip().lower()
            
            if any(term in header_text for term in ['film', 'movie', 'picture', 'production', 'nominee', 'work', 'title']):
                film_col_idx = idx
            elif any(term in header_text for term in ['director', 'actor', 'actress', 'person', 'artist', 'nominee', 'recipient', 'winner']):
                person_col_idx = idx
            # If we find a column that's clearly for year/edition, mark it so we don't confuse it with film
            elif any(term in header_text for term in ['year', 'ceremony', 'edition']):
                # Don't set any index, just note this isn't a film or person column
                continue
    
    # If headers don't help, make a best guess based on content
    if film_col_idx is None or person_col_idx is None:
        rows = table.find_all('tr')
        if len(rows) > 1:  # Skip header row
            cells = rows[1].find_all(['td', 'th'])
            
            # Look for cells with links to movies (often contain "film" in URL)
            for idx, cell in enumerate(cells):
                links = cell.find_all('a')
                for link in links:
                    href = link.get('href', '')
                    if 'film' in href.lower() or ('wiki' in href.lower() and not ('Category:' in href)):
                        if film_col_idx is None:
                            film_col_idx = idx
                            break
            
            # In many Oscar tables, the structure is: Year, Film, Person(s)
            if len(cells) >= 3 and film_col_idx is None:
                # Common pattern: first column is year, second is film, third is person
                film_col_idx = 1
                person_col_idx = 2
            elif len(cells) == 2 and film_col_idx is None:
                # If only two columns, typically first is film and second is person
                film_col_idx = 0
                person_col_idx = 1
    
    return film_col_idx, person_col_idx

def process_category_page(category_name, url):
    """Process a category page to extract nominations"""
    print(f"Processing {category_name}...")
    soup = get_soup(url)
    
    if not soup:
        print(f"Failed to fetch {url}")
        return []
    
    all_nominations = []
    
    # Find content section 
    content = soup.find('div', {'id': 'mw-content-text'})
    if not content:
        return all_nominations
    
    # Find the winners and nominees section header
    winners_header = None
    for header in content.find_all(['h2', 'h3']):
        header_text = header.get_text().lower()
        if 'winners' in header_text and 'nominees' in header_text:
            winners_header = header
            break
    
    # Find all tables after the winners header but before irrelevant tables
    if winners_header:
        # Get all tables in the winners and nominees section
        current_element = winners_header.find_next()
        tables = []
        
        # Get all tables until we reach another main section or irrelevant table
        while current_element and current_element.name != 'h2':
            # Skip sub-sections like "Multiple wins" or "Records"
            if current_element.name == 'h3' and any(x in current_element.get_text().lower() 
                                                   for x in ['multiple', 'records', 'superlatives', 'notes']):
                break
                
            # Collect tables
            if current_element.name == 'table' and 'wikitable' in current_element.get('class', []):
                tables.append(current_element)
            
            # Check if the next element is a table
            if current_element.find('table', class_='wikitable'):
                tables.extend(current_element.find_all('table', class_='wikitable'))
            
            current_element = current_element.find_next()
            
        # Use decade sections if no tables found directly
        if not tables:
            # Try to find tables in decade sections (common pattern)
            decade_headers = content.find_all(['h3', 'h4'])
            for header in decade_headers:
                header_text = header.get_text().lower()
                # Look for decade patterns like "1930s", "2000s", etc.
                if re.search(r'\d{4}s', header_text) or 'decade' in header_text:
                    current = header.find_next()
                    while current and current.name not in ['h3', 'h4', 'h2']:
                        if current.name == 'table' and 'wikitable' in current.get('class', []):
                            tables.append(current)
                        current = current.find_next()
    else:
        # If no winners header found, try to find all tables
        tables = content.find_all('table', class_='wikitable')
    
    # Process each table
    for i, table in enumerate(tables):
        # Skip small tables that might be legend/stats tables
        rows = table.find_all('tr')
        if len(rows) < 3:  # Skip tables with too few rows
            continue
            
        # Skip tables that seem to be about records or stats
        table_text = table.get_text().lower()
        if any(term in table_text for term in ['most wins', 'most nominations', 'multiple', 'records']):
            continue
            
        nominations = process_table(table, category_name, i)
        all_nominations.extend(nominations)
    
    print(f"Extracted {len(all_nominations)} nominations for {category_name}")
    return all_nominations 

def process_table(table, category_name, table_idx):
    """Process a single table to extract nominations"""
    nominations = []
    
    # Get header information for this table
    header_text = get_header_for_table(table)
    
    # Try to extract year from table headers or captions if no header was found
    if not header_text:
        caption = table.find('caption')
        if caption:
            header_text = caption.get_text().strip()
    
    # Extract edition and year from header if available
    edition, ceremony_year = extract_award_edition_info(header_text)
    
    # Get the rows, skipping the header
    rows = table.find_all('tr')
    if len(rows) <= 1:
        return nominations  # Skip tables with only headers
    
    # Determine if there's a header row by checking for th elements
    has_header = len(rows[0].find_all('th')) > 0
    start_row = 1 if has_header else 0
    
    # Find column indexes for film and person
    film_col_idx, person_col_idx = find_film_and_person_columns(table)
    
    if film_col_idx is None:
        print(f"Could not identify film column for table {table_idx+1} in {category_name}")
        # Try to auto-detect columns based on typical structure
        if len(rows) > 1:
            cells = rows[1].find_all(['td', 'th'])
            if len(cells) >= 2:
                # Most Oscar tables have year/award first, then film, then other info
                film_col_idx = 1
                if len(cells) >= 3:
                    person_col_idx = 2
    
    # Determine table type
    table_type = identify_table_type(table)
    
    # Track year groups to determine winners
    current_year = None
    year_nominees_count = 0
    
    # Process rows
    for row_idx, row in enumerate(rows[start_row:], start_row):
        cells = row.find_all(['td', 'th'])
        
        # Skip rows with too few cells
        if len(cells) <= 1:
            continue
        
        # Check if this might be a header row mistakenly placed
        if all('colspan' in str(cell) for cell in cells) or all('rowspan' in str(cell) for cell in cells):
            continue
        
        # Check for year column to detect new year groups
        year_in_row = None
        for cell_idx, cell in enumerate(cells):
            cell_text = cell.get_text().strip()
            year_match = re.search(r'\b(19|20)\d{2}\b', cell_text)
            if year_match and len(cell_text) < 10:  # Short text with a year is likely just the year
                year_in_row = int(year_match.group(0))
                break
                
        # Check if this row represents a new year or continues the previous year
        if year_in_row and year_in_row != current_year:
            current_year = year_in_row
            year_nominees_count = 0
            ceremony_year = current_year
            edition = ceremony_year - 1928
        
        # If we still don't have a year, continue
        if not ceremony_year:
            continue
            
        # Increment nominees count for current year
        year_nominees_count += 1
        
        # In Academy Award tables, the first entry for each year is typically the winner
        # Unless there are explicit winner markers (yellow background, etc.)
        explicit_winner = extract_winner_info(row, table_type)
        is_winner = explicit_winner or (year_nominees_count == 1)
        
        # Film information - if film_col_idx is identified
        film_title = None
        film_url = None
        
        if film_col_idx is not None and film_col_idx < len(cells):
            film_cell = cells[film_col_idx]
            film_title, film_url = extract_film_info(film_cell)
        else:
            # Try to find a cell with a film title by looking for links or bold text
            for idx, cell in enumerate(cells):
                if cell.find('a') or cell.find(['b', 'strong']):
                    potential_title, potential_url = extract_film_info(cell)
                    # Check if this looks like a film title (not a year or person name)
                    if potential_title and not potential_title.isdigit() and len(potential_title) > 3:
                        film_title = potential_title
                        film_url = potential_url
                        film_col_idx = idx
                        break
        
        if not film_title:
            continue
            
        # Person information - if person_col_idx is identified
        people = []
        if person_col_idx is not None and person_col_idx < len(cells):
            person_cell = cells[person_col_idx]
            people = extract_person_info(person_cell)
        else:
            # Try to extract person info from all cells
            for idx, cell in enumerate(cells):
                if idx != film_col_idx:  # Skip the film cell
                    people.extend(extract_person_info(cell))
        
        # Create nomination record
        category_display = category_name.replace('_', ' ')
        nomination = {
            'category': category_display,
            'film_title': film_title,
            'film_url': film_url,
            'people': people,
            'is_winner': is_winner,
            'edition': edition,
            'ceremony_year': ceremony_year
        }
        
        nominations.append(nomination)
    
    return nominations 