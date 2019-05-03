# Import the libraries
import datetime
import pandas as pd
import os
import numpy as np

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

# Replace the missing units of pH with empty strings (necessary for the grouping)
wq_df['unit'].replace(np.nan, '', inplace=True, regex=True)
print(wq_df['unit'].unique())

# Number of rows with negative values
print(str(len(wq_df[wq_df['value'] < 0])) + ' negative values are in the dataset.')

# Keep only rows with positive values
wq_df = wq_df[wq_df['value'] > 0]

# Add a column about the validity of the date
wq_df['ok_date'] = wq_df.apply(check_date, axis=1)
print(wq_df['ok_date'].value_counts())

# Extract only the rows with valid dates
wq_df = wq_df[wq_df['ok_date'] == True]
print(wq_df['ok_date'].value_counts())

# Add the month of the observation as a column
wq_df['month'] = pd.to_datetime(wq_df['date']).dt.month

# Drop unnecessary columns
wq_df.drop(['date', 'divisor', 'multiplier', 'new_code', 'new_desc', 'new_unit', 'ok_date'], axis=1, inplace=True)

# Create a new DF with monthly values of each parameter in each station

# Create a list of columns to use for the grouping
group_cols = list(wq_df)
group_cols.remove('value')

# Create the DF and calculate the count, mean and standard deviation for each group
monthly_df = wq_df.groupby(group_cols)['value'].agg(['count', 'mean', 'std']).reset_index()
print(monthly_df.head())

# Calculate the coefficient of variation (CV) for the observation values of the groups
monthly_df['cv'] = monthly_df['std'] / monthly_df['mean']
print(monthly_df.head())

# Print out the final number of stations
print(str(len(monthly_df['station_id'].unique())) + ' stations remain in the dataset.')

# Write the DF into a CSV
monthly_df.to_csv(os.path.join(dirname, 'full_monthly_data.csv'), sep=';', index=False)

# List of parameters
params = monthly_df['param_code'].unique()
print(params)

# Create a separate output file for each parameter
for param in params:
    param_df = monthly_df[monthly_df['param_code'] == param]
    fname = param + '_monthly_data.csv'
    param_df.to_csv(os.path.join(dirname, 'monthly-water-quality', fname), sep=';', index=False)
