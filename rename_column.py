import pandas as pd

# Read the CSV file
df = pd.read_csv('data/movies.csv')

# Rename the column
if 'runtime' in df.columns:
    df = df.rename(columns={'runtime': 'run_time'})
    # Save back to CSV
    df.to_csv('data/movies.csv', index=False)
    print("Successfully renamed 'runtime' to 'run_time' in movies.csv")
else:
    print("Column 'runtime' not found in movies.csv") 