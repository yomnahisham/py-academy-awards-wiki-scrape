# Academy Awards Web Scraper

This project scrapes Wikipedia for Academy Awards nominations and winners across various categories, spanning all 96 editions of the Academy Awards. The data is structured to match a specific database schema for easy import.

## Overview

The scraper collects detailed information on:
- Movies nominated for Academy Awards
- People associated with nominations (directors, actors, etc.)
- Award categories, editions, and venues
- Production companies, release dates, and other movie details

The data is organized into CSV files and can be imported into a MySQL database.

## Features

- Scrapes 13 major Academy Award categories
- Extracts nomination and winner information
- Collects movie details (runtime, release dates, languages, countries)
- Identifies people associated with nominations and movies
- Follows Wikipedia links to gather additional information
- Respects website crawling etiquette with appropriate delays
- Creates normalized, relational data following the provided schema
- Includes data cleaning and processing scripts
- Provides database import functionality

## Requirements

- Python 3.8+
- MySQL/MariaDB database
- Required Python packages (see requirements.txt):
  - requests>=2.26.0
  - beautifulsoup4>=4.10.0
  - pandas>=1.3.0
  - tqdm>=4.62.0
  - mysql-connector-python (for database operations)

## Installation

1. Clone this repository:
   ```bash
   git clone https://github.com/yourusername/py-web-scrape-wiki.git
   cd py-web-scrape-wiki
   ```

2. Create and activate a virtual environment:
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows, use: .venv\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Set up your environment variables:
   - Copy `.env.example` to `.env`
   - Update the database credentials in `.env`:
     ```
     DB_HOST=localhost
     DB_USER=your_username
     DB_PASSWORD=your_password
     DB_NAME=academy_awards
     DB_CHARSET=utf8mb4
     ```

## Project Structure

- `main.py` - Main script for running the scraper
- `scraper.py` - Core scraping functionality
- `data_processor.py` - Data processing and cleaning
- `movie_extractor.py` - Movie-specific data extraction
- `nomination_extractor.py` - Nomination data extraction
- `import_to_db.py` - Database import functionality
- `import_data.sql` - SQL commands for data import
- Various utility scripts for data cleaning and processing

## Usage

1. Run the main scraper:
   ```bash
   python main.py
   ```

2. Process and clean the data:
   ```bash
   python data_processor.py
   ```

3. Import data to database:
   ```bash
   python import_to_db.py
   ```

## Output Files

The following CSV files will be generated in the `data/` directory:

- `venues.csv` - Award ceremony venues
- `award_editions.csv` - Academy Award editions/ceremonies
- `positions.csv` - Job positions (Director, Actor, etc.)
- `persons.csv` - People involved in nominations
- `award_edition_person.csv` - Relationship between editions and people
- `categories.csv` - Award categories
- `movies.csv` - Movies nominated for awards
- `movie_language.csv` - Languages of movies
- `movie_release_date.csv` - Release dates of movies
- `movie_country.csv` - Countries associated with movies
- `movie_crew.csv` - People who worked on movies and their roles
- `production_company.csv` - Production companies
- `movie_produced_by.csv` - Relationship between movies and production companies
- `nominations.csv` - Award nominations
- `nomination_person.csv` - People associated with nominations

## Database Setup

1. Create a MySQL database named `academy_awards`
2. Update your `.env` file with the correct database credentials
3. Run the import script:
   ```bash
   python import_to_db.py
   ```

## Categories Scraped

- Best Picture
- Best Director
- Best Actor
- Best Actress
- Best Supporting Actor
- Best Supporting Actress
- Best Original Screenplay
- Best Adapted Screenplay
- Best Animated Feature
- Best International Feature Film
- Best Documentary Feature
- Best Original Score
- Best Original Song
- Best Cinematography
- Best Film Editing
- Best Production Design
- Best Costume Design
- Best Makeup and Hairstyling
- Best Sound
- Best Visual Effects

## Notes

- The scraper respects Wikipedia's robots.txt and uses appropriate delays between requests
- Due to the structure of Wikipedia pages, some movie details may be incomplete
- The scraper tries to extract as much information as possible, but may not capture all data
- Running the full scraper may take some time due to the politeness delays
- The complete scraping process took approximately 26 hours (1552.63 minutes) on a standard computer
- Make sure to keep your `.env` file secure and never commit it to version control

## Contributing

Feel free to submit issues and enhancement requests! 