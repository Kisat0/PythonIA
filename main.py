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

df_selected = cleaned_df[['LONGITUDE', 'LATITUDE', 'INSPECTION DATE', 'ACTION', 'CRITICAL FLAG', 'SCORE']]

# in inspection date just keep the year
df_selected['INSPECTION DATE'] = pd.to_datetime(df_selected['INSPECTION DATE'])
df_selected['INSPECTION DATE'] = df_selected['INSPECTION DATE'].dt.year

print(df_selected['INSPECTION DATE'])

# Créer un dictionnaire de mappage
action_mapping = {
    'Violations were cited in the following area(s).': 1,
    'No violations were recorded at the time of this inspection.': 0,
    'Establishment re-opened by DOHMH': 2,
    'Establishment re-closed by DOHMH': 3,
    'Establishment Closed by DOHMH. Violations were cited in the following area(s) and those requiring immediate action were addressed.': 4,
    'Missing': -1
}

# Appliquer le mappage à la colonne Action
df_selected['Action_Code'] = df_selected['Action'].map(action_mapping)

# Créer un dictionnaire de mappage
critical_flag_mapping = {
    'Critical': 1,
    'Not Critical': 0,
    'Not Applicable': -1
}

# Appliquer le mappage à la colonne Critical Flag
df_selected['Critical_Flag_Code'] = df_selected['Critical Flag'].map(critical_flag_mapping)
