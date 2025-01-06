#? Description: This script loads the original dataset and the geocoded dataset, merges them, and cleans the resulting DataFrame.
#! The script address_converter.py must be run before this script to get the geocoded locations from the DOHMH_New_York_City_Restaurant_Inspection_Results.csv file.

import pandas as pd

# Load the original dataset
original_df = pd.read_csv('data/NYC-Restaurant-Inspections/DOHMH_New_York_City_Restaurant_Inspection_Results.csv')

# Load the geocoded dataset
geocoded_df = pd.read_csv('data/NYC-Restaurant-Inspections/Geocoded_Locations.csv')

# Merge the two DataFrames on location columns
merged_df = original_df.merge(
    geocoded_df, 
    on=['BORO', 'STREET', 'BUILDING', 'ZIPCODE'], 
    how='left'  # Use 'left' to retain all rows from the original dataset
)

# Drop rows with missing latitude and longitude values
cleaned_df = merged_df.dropna(subset=['LATITUDE', 'LONGITUDE'])

# Okay now we select the columns we want to keep 

df_selected = cleaned_df[['']]

