#? Description: This script geocodes the addresses of NYC restaurants using the OpenCage Geocoding API.
#! NEED TO BE RUN THE FIRST TIME TO GET THE GEOCODED LOCATIONS FROM THE DOHMH_New_York_City_Restaurant_Inspection_Results.csv

import sys
import pandas as pd
import requests
import time
from dotenv import load_dotenv
import os
import re

# Load environment variables
load_dotenv()

# Constants
INPUT_FILE = 'data/NYC-Restaurant-Inspections/DOHMH_New_York_City_Restaurant_Inspection_Results.csv'
OUTPUT_FILE = 'data/NYC-Restaurant-Inspections/Geocoded_Locations2.0.csv'

PROGRESS_FILE = 'progress.txt'
CHUNK_SIZE = 5000  # Rows to process per day (API rate limit) UwU

# Load the dataset
df = pd.read_csv(INPUT_FILE)

# Extract necessary location columns
df_location = df[['BORO', 'STREET', 'BUILDING', 'ZIPCODE']]

print("Missing values in location data:")
print(df_location.isna().sum())

# Drop rows with missing values in location fields (representing a small fraction of the dataset like just 9 on more than 400k)
df_location = df_location.dropna(subset=['BORO', 'STREET', 'BUILDING', 'ZIPCODE'])


# Add ADDRESS column
def construct_address(line): 
  # Replace spaces with '+' and remove commas
    street = line['STREET'].replace(' ', '+').replace(',', '')
    
    # Use regex to replace multiple '+' with a single '+'
    street = re.sub(r'\++', '+', street)
    
    return f"street={line['BUILDING']}+{street}&city={line['BORO']}&state=NY&postalcode={line['ZIPCODE']}&country=US"

df_location['ADDRESS'] = df_location.apply(construct_address, axis=1)

# Add latitude and longitude columns
df_location['LATITUDE'] = None
df_location['LONGITUDE'] = None

# Function to get GPS coordinates using Geocode API
def get_coordinates(addrs):
    try:
        url = f"{os.getenv('API_URL')}?q={addrs}&api_key={os.getenv('API_KEY')}"
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if data:
                return data[0]['lat'], data[0]['lon']  # Return latitude and longitude
    except requests.exceptions.RequestException as e:
        # Verify if we reached the rate limit by getting a 429 status code
        if response.status_code == 429:
            print("Rate limit exceeded. Please wait and try again later.")
            sys.exit()
        print("Error:", e)
    return None, None

# Function to load progress from file
def load_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
            return int(f.read().strip())
    return 0  # Default to start from the first slot

# Function to save progress to file
def save_progress(progress):
    with open(PROGRESS_FILE, 'w', encoding='utf-8') as f:
        f.write(str(progress))

# Main script
# Load progress
current_day = load_progress()    
start_index = current_day * CHUNK_SIZE
end_index = start_index + CHUNK_SIZE

print(f"Processing rows {start_index} to {end_index - 1}...")

# Extract the current slot
chunk = df_location.iloc[start_index:end_index].copy()

# Fetch GPS coordinates for each row in the chunk
for index, row in chunk.iterrows():
    address = row['ADDRESS']
    lat, lon = get_coordinates(address)
    chunk.at[index, 'LATITUDE'] = lat
    chunk.at[index, 'LONGITUDE'] = lon

    # Delay to respect API rate limits (1 request per second)
    time.sleep(1)

# Save the chunk to the output file
if current_day == 0 and not os.path.exists(OUTPUT_FILE):
    chunk.to_csv(OUTPUT_FILE, mode='w', index=False, header=True)  # Write header for the first chunk
else:
    chunk.to_csv(OUTPUT_FILE, mode='a', index=False, header=False)  # Append subsequent chunks

# Save progress
current_day += 1
save_progress(current_day)

print(f"Finished processing rows {start_index} to {end_index - 1}.")
print(f"Next slot will start from row {end_index}.")
print("Geocoding results saved to:", OUTPUT_FILE)
