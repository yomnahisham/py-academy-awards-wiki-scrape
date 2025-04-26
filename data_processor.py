import os
import pandas as pd
from datetime import datetime
from movie_extractor import parse_person_name
import re

class AcademyAwardsDataProcessor:
    def __init__(self):
        # Create output directory
        os.makedirs('data', exist_ok=True)
        
        # Initialize dataframes for each table
        self.venues_df = pd.DataFrame(columns=['venue_id', 'venue_name', 'neighborhood', 'city', 'state', 'country'])
        self.award_editions_df = pd.DataFrame(columns=['award_edition_id', 'edition', 'aYear', 'cDate', 'venue_id', 'duration', 'network'])
        self.positions_df = pd.DataFrame(columns=['position_id', 'title'])
        self.persons_df = pd.DataFrame(columns=['person_id', 'first_name', 'middle_name', 'last_name', 'birthDate', 'country', 'deathDate'])
        self.award_edition_person_df = pd.DataFrame(columns=['award_edition_id', 'person_id', 'position_id'])
        self.categories_df = pd.DataFrame(columns=['category_id', 'category_name'])
        self.movies_df = pd.DataFrame(columns=['movie_id', 'movie_name', 'run_time'])
        self.movie_language_df = pd.DataFrame(columns=['movie_id', 'in_language'])
        self.movie_release_date_df = pd.DataFrame(columns=['movie_id', 'release_date'])
        self.movie_country_df = pd.DataFrame(columns=['movie_id', 'country'])
        self.movie_crew_df = pd.DataFrame(columns=['movie_id', 'person_id', 'position_id'])
        self.production_company_df = pd.DataFrame(columns=['pd_id', 'company_name'])
        self.movie_produced_by_df = pd.DataFrame(columns=['movie_id', 'production_company_id'])
        self.nominations_df = pd.DataFrame(columns=['nomination_id', 'award_edition_id', 'movie_id', 'category_id', 'won', 'submitted_by'])
        self.nomination_person_df = pd.DataFrame(columns=['nomination_id', 'person_id'])
        
        # Initialize lookup dictionaries for quick access
        self.person_lookup = {}  # name -> person_id
        self.movie_lookup = {}   # title -> movie_id
        self.venue_lookup = {}   # name -> venue_id
        self.category_lookup = {} # name -> category_id
        self.position_lookup = {} # title -> position_id
        self.production_company_lookup = {} # name -> pd_id
        self.award_edition_lookup = {} # (edition, year) -> award_edition_id
        self.next_category_id = 1  # Track next available category ID
        
        # Initialize default values
        self._initialize_default_data()
    
    def _initialize_default_data(self):
        """Initialize default data for positions, venues, etc."""
        # Add common positions
        positions = {
            1: 'Director',
            2: 'Actor',
            3: 'Actress',
            4: 'Supporting Actor',
            5: 'Supporting Actress',
            6: 'Producer',
            7: 'Writer',
            8: 'Editor',
            9: 'Cinematographer',
            10: 'Composer',
            11: 'Singer',
            12: 'Art Director',
            13: 'Visual Effects Artist',
            14: 'Makeup Artist',
            15: 'Hairstylist',
            16: 'Sound Designer',
            17: 'Animator',
            18: 'Host',
            19: 'Presenter'
        }
        
        for position_id, title in positions.items():
            self.add_position(position_id, title)
        
        # Add common venues
        self.add_venue("Dolby Theatre", "Hollywood", "Los Angeles")
        self.add_venue("Kodak Theatre", "Hollywood", "Los Angeles")
        self.add_venue("Shrine Auditorium", "University Park", "Los Angeles")
        self.add_venue("Dorothy Chandler Pavilion", "Downtown", "Los Angeles")
        self.add_venue("Santa Monica Civic Auditorium", "Santa Monica", "Los Angeles")
        self.add_venue("Grauman's Chinese Theatre", "Hollywood", "Los Angeles")
        self.add_venue("Roosevelt Hotel", "Hollywood", "Los Angeles")
    
    def add_position(self, position_id, title):
        """Add position to positions_df"""
        if title in self.position_lookup:
            return self.position_lookup[title]
        
        new_position = pd.DataFrame({'position_id': [position_id], 'title': [title]})
        self.positions_df = pd.concat([self.positions_df, new_position], ignore_index=True)
        self.position_lookup[title] = position_id
        return position_id
    
    def add_category(self, category_name):
        """Add category to categories_df if not exists, and return category_id"""
        # Check if category already exists
        if category_name in self.category_lookup:
            return self.category_lookup[category_name]
        
        # Use the next available category ID
        category_id = self.next_category_id
        self.next_category_id += 1
        
        # Add new category
        new_category = pd.DataFrame({'category_id': [category_id], 'category_name': [category_name]})
        self.categories_df = pd.concat([self.categories_df, new_category], ignore_index=True)
        self.category_lookup[category_name] = category_id
        return category_id
    
    def add_venue(self, venue_name, neighborhood=None, city=None, state=None, country=None):
        """Add venue to venues_df if not already there and return venue_id"""
        if not venue_name:
            return 1  # Default to Dolby Theatre
            
        # Clean venue name
        venue_name = venue_name.strip()
        
        # Check if venue already exists
        if venue_name in self.venue_lookup:
            return self.venue_lookup[venue_name]
            
        # Try to extract city/location information if not provided
        if not city and ',' in venue_name:
            parts = venue_name.split(',')
            venue_part = parts[0].strip()
            if len(parts) > 1:
                location_part = parts[1].strip()
                
                # Common patterns: "Venue, City" or "Venue, City, State"
                if ' ' not in location_part:
                    city = location_part
                elif ',' in location_part:
                    city_state = location_part.split(',')
                    city = city_state[0].strip()
                    if len(city_state) > 1:
                        state = city_state[1].strip()
                else:
                    city = location_part
                
                # Use only the venue part for the name
                venue_name = venue_part
        
        # Add new venue
        new_venue_id = len(self.venues_df) + 1
        
        new_venue = pd.DataFrame({
            'venue_id': [new_venue_id],
            'venue_name': [venue_name],
            'neighborhood': [neighborhood],
            'city': [city or 'Los Angeles'],  # Default to LA if not specified
            'state': [state],
            'country': [country or 'United States']
        })
        
        self.venues_df = pd.concat([self.venues_df, new_venue], ignore_index=True)
        self.venue_lookup[venue_name] = new_venue_id
        return new_venue_id
    
    def add_award_edition(self, edition, year, ceremony_date=None, venue_id=None, network="ABC", duration=None):
        """Add award edition to award_editions_df if not already there and return award_edition_id"""
        key = (edition, year)
        if key in self.award_edition_lookup:
            return self.award_edition_lookup[key]
        
        # Add new award edition
        new_award_edition_id = len(self.award_editions_df) + 1
        
        # If no ceremony date provided, make a reasonable guess
        if ceremony_date is None:
            # Academy Awards typically happen in February/March of the following year
            if year > 1950:  # Modern format
                ceremony_date = f"{int(year) + 1}-02-28"  # Default to Feb 28 of following year
        
        new_award_edition = pd.DataFrame({
            'award_edition_id': [new_award_edition_id],
            'edition': [edition],
            'aYear': [year],
            'cDate': [ceremony_date],
            'venue_id': [venue_id],
            'duration': [duration],
            'network': [network]
        })
        
        self.award_editions_df = pd.concat([self.award_editions_df, new_award_edition], ignore_index=True)
        self.award_edition_lookup[key] = new_award_edition_id
        return new_award_edition_id
    
    def add_person(self, name, birth_date=None, death_date=None, country=None):
        """Add person to persons_df if not already there and return person_id"""
        # Skip movie titles and studio names that might have been passed
        non_person_terms = [
            'studios', 'pictures', 'productions', 'entertainment', 
            'films', 'animation', 'winner', 'nominee', 'award',
            'academy', 'oscar', 'feature', 'documentary'
        ]
        
        if not name or any(term in name.lower() for term in non_person_terms):
            return None
            
        # Check for typical studio name patterns
        studio_patterns = [
            r'.+\b(studios|pictures|productions|films|entertainment)\b.*',
            r'\d{4}',  # Years
            r'.*\b(winner|nominee|award)\b.*'
        ]
        
        if any(re.match(pattern, name.lower()) for pattern in studio_patterns):
            return None
        
        # Check if person already exists
        if name in self.person_lookup:
            return self.person_lookup[name]
        
        first_name, middle_name, last_name = parse_person_name(name)
        
        if not first_name:
            return None
        
        # Add new person
        new_person_id = len(self.persons_df) + 1
        
        new_person = pd.DataFrame({
            'person_id': [new_person_id],
            'first_name': [first_name],
            'middle_name': [middle_name],
            'last_name': [last_name],
            'birthDate': [birth_date],
            'country': [country],
            'deathDate': [death_date]
        })
        
        self.persons_df = pd.concat([self.persons_df, new_person], ignore_index=True)
        self.person_lookup[name] = new_person_id
        return new_person_id
    
    def add_movie(self, title, run_time=None):
        """Add movie to movies_df if not already there and return movie_id"""
        if title in self.movie_lookup:
            return self.movie_lookup[title]
        
        # Add new movie
        new_movie_id = len(self.movies_df) + 1
        
        new_movie = pd.DataFrame({
            'movie_id': [new_movie_id],
            'movie_name': [title],
            'run_time': [run_time]
        })
        
        self.movies_df = pd.concat([self.movies_df, new_movie], ignore_index=True)
        self.movie_lookup[title] = new_movie_id
        return new_movie_id
    
    def add_movie_languages(self, movie_id, languages):
        """Add languages for a movie, handling multiple languages properly"""
        if not languages:
            return
        
        # Convert to list if string provided
        if isinstance(languages, str):
            # Split languages if they're concatenated 
            languages_list = []
            for lang in re.findall(r'[A-Z][a-z]+', languages):
                languages_list.append(lang)
            
            # If nothing was found with the regex, just use the whole string
            if not languages_list and languages.strip():
                languages_list = [languages]
        else:
            languages_list = languages
        
        # Add each language separately
        for language in languages_list:
            language = language.strip()
            if not language:
                continue
            
            # Add to languages dataframe if it doesn't exist yet
            language_id = len(self.movie_language_df) + 1
            
            # Check for duplicates
            language_exists = False
            for _, row in self.movie_language_df.iterrows():
                if row['movie_id'] == movie_id and row['in_language'] == language:
                    language_exists = True
                    break
                
            if not language_exists:
                self.movie_language_df.loc[language_id] = {
                    'movie_id': movie_id,
                    'in_language': language
                }
    
    def add_movie_countries(self, movie_id, countries):
        """Add countries for a movie, handling multiple countries properly"""
        if not countries:
            return
        
        # Convert to list if string provided
        if isinstance(countries, str):
            # Split countries if they're concatenated 
            countries_list = []
            for country in re.findall(r'[A-Z][a-z]+', countries):
                countries_list.append(country)
            
            # Handle multi-word countries
            if not countries_list:
                countries_list = [c.strip() for c in re.split(r'[,/]', countries)]
            
            # If nothing was found with the regex, just use the whole string
            if not countries_list and countries.strip():
                countries_list = [countries]
        else:
            countries_list = countries
        
        # Add each country separately
        for country in countries_list:
            country = country.strip()
            if not country:
                continue
            
            # Add to countries dataframe if it doesn't exist yet
            country_id = len(self.movie_country_df) + 1
            
            # Check for duplicates
            country_exists = False
            for _, row in self.movie_country_df.iterrows():
                if row['movie_id'] == movie_id and row['country'] == country:
                    country_exists = True
                    break
                
            if not country_exists:
                self.movie_country_df.loc[country_id] = {
                    'movie_id': movie_id,
                    'country': country
                }
    
    def add_movie_release_date(self, movie_id, release_date):
        """Add movie release date to movie_release_date_df"""
        # Check if already exists
        existing = self.movie_release_date_df[
            (self.movie_release_date_df['movie_id'] == movie_id) & 
            (self.movie_release_date_df['release_date'] == release_date)
        ]
        
        if not existing.empty:
            return
        
        new_movie_release_date = pd.DataFrame({
            'movie_id': [movie_id],
            'release_date': [release_date]
        })
        
        self.movie_release_date_df = pd.concat([self.movie_release_date_df, new_movie_release_date], ignore_index=True)
    
    def add_production_company(self, company_name):
        """Add production company to production_company_df"""
        if company_name in self.production_company_lookup:
            return self.production_company_lookup[company_name]
        
        # Add new production company
        new_pd_id = len(self.production_company_df) + 1
        
        new_production_company = pd.DataFrame({
            'pd_id': [new_pd_id],
            'company_name': [company_name]
        })
        
        self.production_company_df = pd.concat([self.production_company_df, new_production_company], ignore_index=True)
        self.production_company_lookup[company_name] = new_pd_id
        return new_pd_id
    
    def add_movie_produced_by(self, movie_id, production_company_id):
        """Add movie-production company relationship to movie_produced_by_df"""
        # Check if already exists
        existing = self.movie_produced_by_df[
            (self.movie_produced_by_df['movie_id'] == movie_id) & 
            (self.movie_produced_by_df['production_company_id'] == production_company_id)
        ]
        
        if not existing.empty:
            return
        
        new_movie_produced_by = pd.DataFrame({
            'movie_id': [movie_id],
            'production_company_id': [production_company_id]
        })
        
        self.movie_produced_by_df = pd.concat([self.movie_produced_by_df, new_movie_produced_by], ignore_index=True)
    
    def add_movie_crew(self, movie_id, person_id, position_id):
        """Add movie-crew relationship to movie_crew_df"""
        # Check if already exists
        existing = self.movie_crew_df[
            (self.movie_crew_df['movie_id'] == movie_id) & 
            (self.movie_crew_df['person_id'] == person_id) &
            (self.movie_crew_df['position_id'] == position_id)
        ]
        
        if not existing.empty:
            return
        
        new_movie_crew = pd.DataFrame({
            'movie_id': [movie_id],
            'person_id': [person_id],
            'position_id': [position_id]
        })
        
        self.movie_crew_df = pd.concat([self.movie_crew_df, new_movie_crew], ignore_index=True)
    
    def add_nomination(self, award_edition_id, movie_id, category_id, won=False, submitted_by=None):
        """Add nomination to nominations_df and return nomination_id"""
        if not movie_id or not award_edition_id:
            return None
        
        # Add new nomination
        new_nomination_id = len(self.nominations_df) + 1
        
        new_nomination = pd.DataFrame({
            'nomination_id': [new_nomination_id],
            'award_edition_id': [award_edition_id],
            'movie_id': [movie_id],
            'category_id': [category_id],
            'won': [1 if won else 0],
            'submitted_by': [submitted_by]
        })
        
        self.nominations_df = pd.concat([self.nominations_df, new_nomination], ignore_index=True)
        return new_nomination_id
    
    def add_nomination_person(self, nomination_id, person_id):
        """Add nomination-person relationship to nomination_person_df"""
        # Check if already exists
        existing = self.nomination_person_df[
            (self.nomination_person_df['nomination_id'] == nomination_id) & 
            (self.nomination_person_df['person_id'] == person_id)
        ]
        
        if not existing.empty:
            return
        
        new_nomination_person = pd.DataFrame({
            'nomination_id': [nomination_id],
            'person_id': [person_id]
        })
        
        self.nomination_person_df = pd.concat([self.nomination_person_df, new_nomination_person], ignore_index=True)
    
    def add_award_edition_person(self, award_edition_id, person_id, position_id):
        """Add relationship between award edition and a person in a specific role"""
        # Check if already exists
        existing = self.award_edition_person_df[
            (self.award_edition_person_df['award_edition_id'] == award_edition_id) & 
            (self.award_edition_person_df['person_id'] == person_id) &
            (self.award_edition_person_df['position_id'] == position_id)
        ]
        
        if not existing.empty:
            return
        
        new_award_edition_person = pd.DataFrame({
            'award_edition_id': [award_edition_id],
            'person_id': [person_id],
            'position_id': [position_id]
        })
        
        self.award_edition_person_df = pd.concat([self.award_edition_person_df, new_award_edition_person], ignore_index=True)
    
    def process_movie_details(self, movie_id, movie_details):
        """Process movie details and add them to the database"""
        if not movie_details:
            return
        
        # Update runtime if available
        if movie_details.get('runtime'):
            self.movies_df.loc[self.movies_df['movie_id'] == movie_id, 'runtime'] = movie_details['runtime']
        
        # Add languages
        languages = movie_details.get('languages', [])
        if languages:
            self.add_movie_languages(movie_id, languages)
        
        # Add countries
        countries = movie_details.get('countries', [])
        if countries:
            self.add_movie_countries(movie_id, countries)
        
        # Add release dates
        for release_date in movie_details.get('release_dates', []):
            self.add_movie_release_date(movie_id, release_date)
        
        # Add directors
        for director in movie_details.get('directors', []):
            # Handle both string format and tuple format (name, url)
            if isinstance(director, tuple):
                director_name = director[0]
            else:
                director_name = director
            
            person_id = self.add_person(director_name)
            if person_id:
                position_id = self.position_lookup.get('Director', 1)
                self.add_movie_crew(movie_id, person_id, position_id)
        
        # Add producers
        for producer in movie_details.get('producers', []):
            # Handle both string format and tuple format (name, url)
            if isinstance(producer, tuple):
                producer_name = producer[0]
            else:
                producer_name = producer
            
            person_id = self.add_person(producer_name)
            if person_id:
                position_id = self.position_lookup.get('Producer', 6)
                self.add_movie_crew(movie_id, person_id, position_id)
        
        # Add writers
        for writer in movie_details.get('writers', []):
            # Handle both string format and tuple format (name, url)
            if isinstance(writer, tuple):
                writer_name = writer[0]
            else:
                writer_name = writer
            
            person_id = self.add_person(writer_name)
            if person_id:
                position_id = self.position_lookup.get('Writer', 7)
                self.add_movie_crew(movie_id, person_id, position_id)
        
        # Add editors
        for editor in movie_details.get('editors', []):
            # Handle both string format and tuple format (name, url)
            if isinstance(editor, tuple):
                editor_name = editor[0]
            else:
                editor_name = editor
            
            person_id = self.add_person(editor_name)
            if person_id:
                position_id = self.position_lookup.get('Editor', 8)
                self.add_movie_crew(movie_id, person_id, position_id)
        
        # Add cinematographers
        for cinematographer in movie_details.get('cinematographers', []):
            # Handle both string format and tuple format (name, url)
            if isinstance(cinematographer, tuple):
                cinematographer_name = cinematographer[0]
            else:
                cinematographer_name = cinematographer
            
            person_id = self.add_person(cinematographer_name)
            if person_id:
                position_id = self.position_lookup.get('Cinematographer', 9)
                self.add_movie_crew(movie_id, person_id, position_id)
        
        # Add composers
        for composer in movie_details.get('composers', []):
            # Handle both string format and tuple format (name, url)
            if isinstance(composer, tuple):
                composer_name = composer[0]
            else:
                composer_name = composer
            
            person_id = self.add_person(composer_name)
            if person_id:
                position_id = self.position_lookup.get('Composer', 10)
                self.add_movie_crew(movie_id, person_id, position_id)
        
        # Add cast (for backward compatibility)
        for actor_name in movie_details.get('cast', []):
            # Handle both string format and tuple format (name, url)
            if isinstance(actor_name, tuple):
                name = actor_name[0]
            else:
                name = actor_name
            
            person_id = self.add_person(name)
            if person_id:
                # Use Actor position by default
                position_id = self.position_lookup.get('Actor', 2)
                self.add_movie_crew(movie_id, person_id, position_id)
        
        # Add production companies
        for company_name in movie_details.get('production_companies', []):
            company_id = self.add_production_company(company_name)
            self.add_movie_produced_by(movie_id, company_id)
    
    def process_nomination(self, nomination_data, movie_details=None):
        """Process a nomination and add it to the database"""
        # Extract nomination data
        category_name = nomination_data.get('category')
        film_title = nomination_data.get('film_title')
        is_winner = nomination_data.get('is_winner', False)
        edition = nomination_data.get('edition')
        ceremony_year = nomination_data.get('ceremony_year')
        people = nomination_data.get('people', [])
        
        # Skip if essential data is missing
        if not film_title or not category_name:
            return
        
        # Add category if it doesn't exist
        category_id = self.add_category(category_name)
        
        # Add the movie
        movie_id = self.add_movie(film_title)
        
        # Process movie details if available
        if movie_details and movie_id:
            self.process_movie_details(movie_id, movie_details)
        
        # Add or get award edition
        venue_id = 1  # Default to Dolby Theatre
        award_edition_id = self.add_award_edition(edition, ceremony_year, None, venue_id)
        
        # Create the nomination
        nomination_id = self.add_nomination(award_edition_id, movie_id, category_id, is_winner)
        
        # Add people associated with the nomination
        for person_name, person_url in people:
            # Only process valid person names
            if person_name:
                person_id = self.add_person(person_name)
                
                # Only add person to nomination if the person_id is valid
                if person_id:
                    self.add_nomination_person(nomination_id, person_id)
                    
                    # Try to determine position based on category
                    position_id = None
                    category_lower = category_name.lower()
                    if 'actor' in category_lower:
                        position_id = 2  # Actor
                    elif 'actress' in category_lower:
                        position_id = 3  # Actress
                    elif 'supporting actor' in category_lower:
                        position_id = 4  # Supporting Actor
                    elif 'supporting actress' in category_lower:
                        position_id = 5  # Supporting Actress
                    elif 'director' in category_lower:
                        position_id = 1  # Director
                    elif 'screenplay' in category_lower or 'writing' in category_lower:
                        position_id = 7  # Writer
                    elif 'cinematography' in category_lower:
                        position_id = 9  # Cinematographer
                    elif 'editing' in category_lower:
                        position_id = 8  # Editor
                    elif 'score' in category_lower or 'music' in category_lower:
                        position_id = 10  # Composer
                    elif 'song' in category_lower:
                        position_id = 11  # Singer
                    elif 'product' in category_lower or 'art' in category_lower or 'design' in category_lower:
                        position_id = 12  # Art Director
                    elif 'effect' in category_lower or 'visual' in category_lower:
                        position_id = 13  # Visual Effects Artist
                    elif 'makeup' in category_lower:
                        position_id = 14  # Makeup Artist
                    elif 'hair' in category_lower:
                        position_id = 15  # Hairstylist
                    elif 'sound' in category_lower:
                        position_id = 16  # Sound Designer
                    elif 'animat' in category_lower:
                        position_id = 17  # Animator
                    else:
                        position_id = 6  # Default to Producer for Best Picture
                    
                    # Associate person with appropriate position in movie
                    if position_id and movie_id and person_id:
                        self.add_movie_crew(movie_id, person_id, position_id)
    
    def save_to_csv(self):
        """Save all dataframes to CSV files"""
        print(f"Saving {len(self.venues_df)} venues...")
        self.venues_df.to_csv('data/venues.csv', index=False)
        
        print(f"Saving {len(self.award_editions_df)} award editions...")
        self.award_editions_df.to_csv('data/award_editions.csv', index=False)
        
        print(f"Saving {len(self.positions_df)} positions...")
        self.positions_df.to_csv('data/positions.csv', index=False)
        
        print(f"Saving {len(self.persons_df)} persons...")
        self.persons_df.to_csv('data/persons.csv', index=False)
        
        print(f"Saving {len(self.award_edition_person_df)} award edition persons...")
        self.award_edition_person_df.to_csv('data/award_edition_person.csv', index=False)
        
        print(f"Saving {len(self.categories_df)} categories...")
        self.categories_df.to_csv('data/categories.csv', index=False)
        
        print(f"Saving {len(self.movies_df)} movies...")
        self.movies_df.to_csv('data/movies.csv', index=False)
        
        print(f"Saving {len(self.movie_language_df)} movie languages...")
        self.movie_language_df.to_csv('data/movie_language.csv', index=False)
        
        print(f"Saving {len(self.movie_release_date_df)} movie release dates...")
        self.movie_release_date_df.to_csv('data/movie_release_date.csv', index=False)
        
        print(f"Saving {len(self.movie_country_df)} movie countries...")
        self.movie_country_df.to_csv('data/movie_country.csv', index=False)
        
        print(f"Saving {len(self.movie_crew_df)} movie crew entries...")
        self.movie_crew_df.to_csv('data/movie_crew.csv', index=False)
        
        print(f"Saving {len(self.production_company_df)} production companies...")
        self.production_company_df.to_csv('data/production_company.csv', index=False)
        
        print(f"Saving {len(self.movie_produced_by_df)} movie production company relationships...")
        self.movie_produced_by_df.to_csv('data/movie_produced_by.csv', index=False)
        
        print(f"Saving {len(self.nominations_df)} nominations...")
        self.nominations_df.to_csv('data/nominations.csv', index=False)
        
        print(f"Saving {len(self.nomination_person_df)} nomination person relationships...")
        self.nomination_person_df.to_csv('data/nomination_person.csv', index=False)
        
        print("All data saved successfully!")
    
    def print_stats(self):
        """Print statistics about the data"""
        print("\n=== Database Statistics ===")
        print(f"Total movies: {len(self.movies_df)}")
        print(f"Total persons: {len(self.persons_df)}")
        print(f"Total nominations: {len(self.nominations_df)}")
        print(f"Total winning nominations: {self.nominations_df['won'].sum()}")
        print(f"Total categories: {len(self.categories_df)}")
        print(f"Total award editions: {len(self.award_editions_df)}")
        print("==========================\n")
    
    def update_person_info(self, person_name, person_details):
        """Update details for a person"""
        if not person_name:
            return
        
        # Find the person by name
        person_id = None
        
        # Check which columns to use for comparison
        for idx, row in self.persons_df.iterrows():
            matched = False
            
            # Try comparing with first_name
            if 'first_name' in row:
                first_name, _, last_name = parse_person_name(person_name)
                if row['first_name'] == first_name:
                    if last_name and 'last_name' in row and row['last_name'] == last_name:
                        matched = True
                    elif not last_name:
                        matched = True
            
            # Try comparing with name if available
            if not matched and 'name' in row and row['name'] == person_name:
                matched = True
                
            if matched:
                person_id = row['person_id']
                break
            
        if not person_id:
            # If person doesn't exist, add them
            person_id = self.add_person(person_name)
            if not person_id:
                return
        
        # Update the details we have
        if 'birth_date' in person_details and person_details['birth_date']:
            self.persons_df.loc[self.persons_df['person_id'] == person_id, 'birthdate'] = person_details['birth_date']
        
        if 'birth_place' in person_details and person_details['birth_place']:
            self.persons_df.loc[self.persons_df['person_id'] == person_id, 'birthplace'] = person_details['birth_place']
        
        if 'death_date' in person_details and person_details['death_date']:
            self.persons_df.loc[self.persons_df['person_id'] == person_id, 'deathdate'] = person_details['death_date']
        
        if 'occupation' in person_details and person_details['occupation']:
            # Store occupation in notes field
            occupation = person_details['occupation']
            if isinstance(occupation, list):
                occupation = ', '.join(occupation)
            self.persons_df.loc[self.persons_df['person_id'] == person_id, 'notes'] = occupation
        
        # Add gender if available
        if 'gender' in person_details and person_details['gender']:
            self.persons_df.loc[self.persons_df['person_id'] == person_id, 'gender'] = person_details['gender']
            
        return person_id 