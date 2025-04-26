-- SQL Script to import the Academy Awards data from CSV files
-- Make sure to adjust the file paths if your files are in a different location

-- Import venues first (no foreign key dependencies)
LOAD DATA INFILE 'data/venues.csv'
INTO TABLE venue
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(venue_id, venue_name, @neighborhood, @city, @state, @country)
SET
neighborhood = NULLIF(@neighborhood, ''),
city = NULLIF(@city, ''),
state = NULLIF(@state, ''),
country = NULLIF(@country, '');

-- Import positions
LOAD DATA INFILE 'data/positions.csv'
INTO TABLE positions
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(position_id, title);

-- Import persons
LOAD DATA INFILE 'data/persons.csv'
INTO TABLE person
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(person_id, first_name, @middle_name, last_name, @birthDate, @country, @deathDate)
SET
middle_name = NULLIF(@middle_name, ''),
birthDate = NULLIF(@birthDate, ''),
country = NULLIF(@country, ''),
deathDate = NULLIF(@deathDate, '');

-- Import award editions
LOAD DATA INFILE 'data/award_editions.csv'
INTO TABLE award_edition
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(award_edition_id, edition, @aYear, @cDate, @venue_id, @duration, @network)
SET
aYear = NULLIF(@aYear, ''),
cDate = NULLIF(@cDate, ''),
venue_id = NULLIF(@venue_id, ''),
duration = NULLIF(@duration, ''),
network = NULLIF(@network, '');

-- Import categories
LOAD DATA INFILE 'data/categories.csv'
INTO TABLE category
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(category_id, category_name);

-- Import movies
LOAD DATA INFILE 'data/movies.csv'
INTO TABLE movie
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(movie_id, movie_name, @run_time)
SET
run_time = NULLIF(@run_time, '');

-- Import award_edition_person
LOAD DATA INFILE 'data/award_edition_person.csv'
INTO TABLE award_edition_person
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(award_edition_id, person_id, position_id);

-- Import movie languages
LOAD DATA INFILE 'data/movie_language.csv'
INTO TABLE movie_language
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(movie_id, in_language);

-- Import movie release dates
LOAD DATA INFILE 'data/movie_release_date.csv'
INTO TABLE movie_release_date
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(movie_id, release_date);

-- Import movie countries
LOAD DATA INFILE 'data/movie_country.csv'
INTO TABLE movie_country
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(movie_id, country);

-- Import production companies
LOAD DATA INFILE 'data/production_company.csv'
INTO TABLE production_company
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(pd_id, company_name);

-- Import movie_produced_by
LOAD DATA INFILE 'data/movie_produced_by.csv'
INTO TABLE movie_produced_by
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(movie_id, production_company_id);

-- Import movie crew
LOAD DATA INFILE 'data/movie_crew.csv'
INTO TABLE movie_crew
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(movie_id, person_id, position_id);

-- Import nominations
LOAD DATA INFILE 'data/nominations.csv'
INTO TABLE nomination
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(nomination_id, award_edition_id, movie_id, category_id, won, @submitted_by)
SET
submitted_by = NULLIF(@submitted_by, '');

-- Import nomination_person
LOAD DATA INFILE 'data/nomination_person.csv'
INTO TABLE nomination_person
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(nomination_id, person_id);

-- Display completion message
SELECT 'Data import completed successfully!' AS Status; 