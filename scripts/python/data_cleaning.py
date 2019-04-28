# Import the libraries
import datetime
import pandas as pd
import os

# Define a function to check if the date is valid
def check_date(row):
    correct_date = None
    # Split the date into strings
    strings = row['date'].split('-')
    # Test the date
    try:
        # If the date is not in the correct format this will throw an IndexError
        year, month, day = int(strings[0]), int(strings[1]), int(strings[2])
        # If the date is not valid this will throw a ValueError
        datetime.datetime(year, month, day)
        correct_date = True
    # If the previous block throws an error
    except (IndexError, ValueError):
        correct_date = False
    return correct_date

# Import the water quality datasets
dirname = 'C:/Users/Holger/PycharmProjects/water-quality-modeling/data'
gemstat = pd.read_csv(os.path.join(dirname, 'gemstat/gemstat.csv'), sep=';')
waterbase = pd.read_csv(os.path.join(dirname, 'waterbase/waterbase.csv'), sep=';')
glorich = pd.read_csv(os.path.join(dirname, 'glorich/glorich.csv'), sep=';')

# Import the file with the mapped parameters
map_df = pd.read_csv(os.path.join(dirname, 'data_map.csv'), sep=';')
print(map_df)

# Extract the rows for each dataset
gemstat_map = map_df[map_df['origin'] == 'GEMStat']
waterbase_map = map_df[map_df['origin'] == 'Waterbase']
glorich_map = map_df[map_df['origin'] == 'GLORICH']

# Drop the column with origin
gemstat_map.drop(['origin'], axis=1, inplace=True)
waterbase_map.drop(['origin'], axis=1, inplace=True)
glorich_map.drop(['origin'], axis=1, inplace=True)

# Merge the maps with the water quality data
gemstat = gemstat.merge(gemstat_map, on='param_code')
waterbase = waterbase.merge(waterbase_map, on='param_code')
glorich = glorich.merge(glorich_map, on='param_code')

# Concat the three datasets
wq_df = pd.concat([gemstat, waterbase, glorich])
print(wq_df.head())
print(wq_df.dtypes)

# Only extract the parameters that have been mapped and have values in the 'new_code' column
wq_df = wq_df[pd.notnull(wq_df['new_code'])]

# Print out the number of stations
print(str(len(wq_df['station_id'].unique())) + ' stations are in the dataset.')

# Calculate new values for parameters with units other than mg/l (umol/l and ug/l)
wq_df['value'] = wq_df['value'] / wq_df['divisor'] * wq_df['multiplier']

# Change the codes, descriptions and units based on the mapping
wq_df['param_code'] = wq_df['new_code']
wq_df['param_desc'] = wq_df['new_desc']
wq_df['unit'] = wq_df['new_unit']
print(wq_df.head())
print(wq_df['unit'].unique())

# Number of rows with negative values
print(len(wq_df[wq_df['value'] < 0]))

# Keep only rows with positive values
wq_df = wq_df[wq_df['value'] > 0]

# Add a column about the validity of the date
wq_df['ok_date'] = wq_df.apply(check_date, axis=1)
print(wq_df['ok_date'].value_counts())

# Extract only the rows with valid dates
wq_df = wq_df[wq_df['ok_date'] == True]
print(wq_df['ok_date'].value_counts())

# Print out the final number of stations
print(str(len(wq_df['station_id'].unique())) + ' stations remain in the dataset.')

# Create a new DF with proper column names and write into a CSV
out_df = pd.DataFrame(
    {
        'lat': wq_df['lat'],
        'lon': wq_df['lon'],
        'date': wq_df['date'],
        'station_id': wq_df['station_id'],
        'param_code': wq_df['param_code'],
        'param_desc': wq_df['param_desc'],
        'value': wq_df['value'],
        'unit': wq_df['unit'],
        'origin': wq_df['origin']
    }
)
out_df.to_csv(os.path.join(dirname, 'water_quality.csv'), sep=';', index=False)
