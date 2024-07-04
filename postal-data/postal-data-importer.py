import pandas as pd
import psycopg2
from dotenv import load_dotenv
import os
import logging

# Configure logging (optional)
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(message)s')

# Load environment variables from .env file
load_dotenv()

# Read the CSV file
file_path = os.environ.get('POSTAL_DATA_FILE_PATH')
df = pd.read_csv(file_path, dtype={
    'zip': str,
    'lat': float,
    'lng': float,
    'city': str,
    'state_id': str,
    'state_name': str,
    'parent_zcta': str,
    'county_name': str,
    'timezone': str
})

# Ensure population is of integer type
df['population'] = df['population'].apply(pd.to_numeric, errors='coerce').fillna(0).astype(int)

number_of_rows = df.shape[0]
logging.info(f"Read {number_of_rows} rows from the CSV file.")

# Get PostgreSQL connection details
dbname = os.environ.get('POSTGRES_DB')
user = os.environ.get('POSTGRES_USER')
password = os.environ.get('POSTGRES_PASSWORD')
host = os.environ.get('POSTGRES_HOST')
port = os.environ.get('POSTGRES_PORT')

logging.info("Connecting to the database...")

try:
  # Connect to PostgreSQL
  conn = psycopg2.connect(
      dbname=dbname,
      user=user,
      password=password,
      host=host,
      port=port
  )
except psycopg2.Error as e:  # Catch psycopg2 specific errors for connection issues
  logging.error(f"Error connecting to database: {e}")
  raise  # Re-raise the exception to abort the script

logging.info("Connected to the database successfully!")

try:
  # Create a cursor object
  cur = conn.cursor()

  logging.info("Creating 'postal_data' tale if does not exist.")

  # Create the table "postal_data"
  create_table_query = """
  CREATE TABLE IF NOT EXISTS postal_data (
      zip varchar PRIMARY KEY,
      lat numeric,
      lng numeric,
      city varchar,
      state_id varchar,
      state_name varchar,
      population int,
      county_name varchar,
      timezone varchar
  );
  """

  cur.execute(create_table_query)
  conn.commit()

  # Define the insert query
  insert_query = """
  INSERT INTO postal_data (
      zip, lat, lng, city, state_id, state_name,
      population, county_name, timezone
  ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
  ON CONFLICT (zip) DO NOTHING;
  """

  logging.info("Inserting data into the database...")
  # Iterate over the rows and insert data
  row_count = 0
  for index, row in df.iterrows():
      cur.execute(insert_query, (
          row['zip'], row['lat'], row['lng'], row['city'], row['state_id'], row['state_name'],
          row['population'], row['county_name'], row['timezone']
      ))
      row_count += 1

  # Commit the transaction
  conn.commit()
  logging.info(f"Data inserted successfully! Inserted {row_count} rows.")

except Exception as e:
  logging.error(f"Error inserting data: {e}")

# Close the cursor and connection
finally:
  if conn:
    cur.close()
    conn.close()
    logging.info("Connection and cursor closed.")
