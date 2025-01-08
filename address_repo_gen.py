import pandas as pd
import time
import json
from geopandas.tools import geocode

# Constants
INPUT_FILE = 'data/NYC-Restaurant-Inspections/DOHMH_New_York_City_Restaurant_Inspection_Results.csv'
OUTPUT_FILE = 'data/restaurants_list.geojson'
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds
CHUNK_SIZE = 100
ROW_LIMIT = 10
START_INDEX = 0 

# Function to construct address
def construct_address(line):
    return f"{line['BUILDING']} {line['STREET']}, {line['BORO']}, NY {line['ZIPCODE']}"

# Function to geocode address
def geocode_address(address):
    for attempt in range(MAX_RETRIES):
        try:
            print(address)
            location = geocode(address, timeout=10)  # Increase timeout to 10 seconds
            print(location.geometry.iloc[0].y, location.geometry.iloc[0].x)
            return location.geometry.iloc[0].y, location.geometry.iloc[0].x  # Latitude, Longitude
        except Exception as e:
            print(f"Attempt {attempt + 1} failed for address: {address}")
            print(e)
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)  # Wait before retrying
            else:
                print(f"Failed to geocode after {MAX_RETRIES} attempts: {address}")
                return None, None

df_location = pd.read_csv(INPUT_FILE)
df_location = df_location[['BORO', 'STREET', 'BUILDING', 'ZIPCODE', 'CUISINE DESCRIPTION', 'GRADE', 'PHONE', 'INSPECTION DATE', 'VIOLATION CODE', 'VIOLATION DESCRIPTION', 'SCORE']]

# Drop rows with missing values in location fields
df_location = df_location.dropna(subset=['BORO', 'STREET', 'BUILDING', 'ZIPCODE'])

# Add latitude and longitude columns
df_location['LATITUDE'] = None
df_location['LONGITUDE'] = None

processed_rows = 0
for start in range(START_INDEX, len(df_location), CHUNK_SIZE):
    if processed_rows >= ROW_LIMIT:
        break

    chunk = df_location.iloc[start:start + CHUNK_SIZE].copy()

    # print the current status
    print(f"Processing rows {start} to {start + CHUNK_SIZE}...")

    # Construct addresses
    chunk['ADDRESS'] = chunk.apply(construct_address, axis=1)

    # Geocode addresses
    chunk[['LATITUDE', 'LONGITUDE']] = chunk['ADDRESS'].apply(geocode_address).apply(pd.Series)

    # Update the main DataFrame with the geocoded chunk
    df_location.update(chunk)

    processed_rows += len(chunk)

# Create json structure
features = []
for _, row in df_location.iterrows():
    if pd.notnull(row['LATITUDE']) and pd.notnull(row['LONGITUDE']):
        feature = {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [row['LONGITUDE'], row['LATITUDE']]
            },
            "properties": {
                "name": f"Restaurant {row.name}",
                "cuisine": row['CUISINE DESCRIPTION'],
                "address": row['BUILDING'] + ' ' + row['STREET'] + ', ' + row['BORO'] + ', NY ' + str(row['ZIPCODE']),
                "phone": row['PHONE'],
                "inspection_date": row['INSPECTION DATE'],
                "violation_code": row['VIOLATION CODE'],
                "violation_description": row['VIOLATION DESCRIPTION'],
                "score": row['SCORE'],
            }
        }
        features.append(feature)

geojson = {
    "type": "FeatureCollection",
    "features": features
}

# Save json data
with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
    json.dump(geojson, f)
    
# Print the final status
print(f"Processed {processed_rows} rows.")
