import time
import pandas as pd
import duckdb
from sqlalchemy import create_engine
import psycopg2
import settings

# PostgreSQL connection details
POSTGRES_USER = settings.POSTGRES_USER
POSTGRES_PASSWORD = settings.POSTGRES_PASSWORD
POSTGRES_HOST = settings.POSTGRES_HOST
POSTGRES_PORT = settings.POSTGRES_PORT
POSTGRES_DB = settings.POSTGRES_DB

# Create the PostgreSQL connection string
POSTGRES_CONNECTION_STRING = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# CSV file path
CSV_FILE_PATH = settings.CSV_FILE_PATH
# Define the fucntion to use to read data and load into Postgres database
def load_with_pandas():
    print("Starting load with Pandas...")
    start_time = time.time()
    # Read the CSV file using Pandas
    df = pd.read_csv(CSV_FILE_PATH) 
    read_time = time.time()
    print(f"Time to read CSV with Pandas: {read_time - start_time:.2f} seconds")
    
    # Filter out only movies with a vote_count >20000 and with the status ='Released'
    filtered_df = df[(df['vote_count'] > 20000) & (df['status'] == 'Released')]
    filter_time = time.time()
    print(f"Time to filter data with Pandas: {filter_time - read_time:.2f} seconds")
    print(f"Number of records after filtering: {len(filtered_df)}")
    
    # **Select the top 10 Movies by vote_average**
    top_movies = filtered_df.sort_values(by='vote_average', ascending=False).head(10)
    top_time = time.time()
    print(f"Time to select top 10 Movies by Rating with Pandas: {top_time - filter_time:.2f} seconds")
    print(f"The Top 10 by Rating are :\n")
    for movie in top_movies['title']:  
        #Print the top 10 movies by rating
        print(movie)
    # Load the data into PostgreSQL
    engine = create_engine(POSTGRES_CONNECTION_STRING)
    top_movies.to_sql(
        name='top_movies_by_rating_pandas',
        con=engine,
        if_exists='replace',  # Replace table if it exists
        index=False,
        method='multi'
    )
    load_time = time.time()
    print(f"Time to load data into PostgreSQL with Pandas: {load_time - top_time:.2f} seconds")
    print(f"Total time with Pandas: {load_time - start_time:.2f} seconds\n")
# Define the fucntion to use to read data and load into Postgres database with Duckdb
def load_with_duckdb():
    print("Starting load with DuckDB...")
    start_time = time.time()
    # Use DuckDB to read, filter, and select top 10 movies from the CSV file
    con = duckdb.connect(database=':memory:')
    query = f"""
    SELECT *
    FROM read_csv_auto('{CSV_FILE_PATH}')
    WHERE vote_count > 20000 AND status = 'Released'
    ORDER BY vote_average DESC
    LIMIT 10
    """
    df = con.execute(query).df()
    process_time = time.time()
    print(f"Time to read, filter, and select top 10 movies with DuckDB: {process_time - start_time:.2f} seconds")
    print(f"Number of records after processing: {len(df)}")
    print(f"The Top 10 by Rating are :\n")
    for movie in df['title']:  
        #Print the top 10 movies by rating
        print(movie)
    
    # Load data into PostgreSQL
    engine = create_engine(POSTGRES_CONNECTION_STRING)
    df.to_sql(
        name='top_movies_by_rating_duckdb',
        con=engine,
        if_exists='replace',  # Replace table if it exists
        index=False,
        method='multi',
        chunksize=10000
    )
    load_time = time.time()
    print(f"Time to load data into PostgreSQL with DuckDB: {load_time - process_time:.2f} seconds")
    print(f"Total time with DuckDB: {load_time - start_time:.2f} seconds\n")

if __name__ == '__main__':
    load_with_pandas() 
    load_with_duckdb() 
