# Academy Awards Web Scraper

This project scrapes Wikipedia for Academy Awards nominations and winners across various categories, spanning all 96 editions of the Academy Awards. The data is structured to match a specific database schema for easy import.

## Overview

The scraper collects detailed information on:
- Movies nominated for Academy Awards
- People associated with nominations (directors, actors, etc.)
- Award categories, editions, and venues
- Production companies, release dates, and other movie details

The data is organized into CSV files, making it easy to import into a MySQL database.

## Features

- Scrapes 13 major Academy Award categories
- Extracts nomination and winner information
- Collects movie details (runtime, release dates, languages, countries)
- Identifies people associated with nominations and movies
- Follows Wikipedia links to gather additional information
- Respects website crawling etiquette with appropriate delays
- Creates normalized, relational data following the provided schema

## Requirements

- Python 3.8+
- Required packages:
  - requests
  - beautifulsoup4
  - pandas
  - tqdm

## Installation

1. Clone this repository
2. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

## Usage

Run the main script:
```
python main.py
```

The script will:
1. Scrape all the Academy Award categories
2. Extract nomination data
3. Follow links to gather additional details
4. Process and organize the data
5. Save everything to CSV files in the `data/` directory

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

## Data Import

After running the scraper, you can import the CSV files into your database using one of these methods:

### Method 1: Using the import script

The `import_to_db.py` script provides an easy way to import all data at once:

```
pip install mysql-connector-python
python import_to_db.py --db-url mysql://username:password@hostname:port/database
```

Replace `username`, `password`, `hostname`, `port`, and `database` with your MySQL/MariaDB connection details.

### Method 2: Using MySQL's LOAD DATA command

You can also use the `import_data.sql` script which contains LOAD DATA commands for each table:

```
mysql -u username -p database_name < import_data.sql
```

Make sure to update the file paths in the script if your CSV files are in a different location.

## Notes

- The scraper respects Wikipedia's robots.txt and uses appropriate delays between requests
- Due to the structure of Wikipedia pages, some movie details may be incomplete
- The scraper tries to extract as much information as possible, but may not capture all data
- Running the full scraper may take some time due to the politeness delays

## Categories Scraped

- Best Makeup and Hairstyling
- Best Documentary Feature Film
- Best Original Score
- Best Original Song
- Best Documentary Short Film
- Best Picture
- Best Animated Feature
- Best Visual Effects
- Best Adapted Screenplay
- Best Film Editing
- Best Production Design
- Best Animated Short Film
- Best Sound 