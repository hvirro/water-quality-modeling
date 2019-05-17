# Import the libraries
import pandas as pd
import os

# Define a function to create a DF from a list of DFs and print out basic information
def create_df(df_list):
    df = pd.concat(df_list)
    df.drop_duplicates(inplace=True)
    df.reset_index(drop=True, inplace=True)
    print('\n')
    print(df.head())
    print('\n')
    print(df.dtypes)
    print('\n')
    return df

# Location of the Excel files
path = 'C:/Users/Holger/PycharmProjects/water-quality-modeling/data/gemstat'

# Create a list of the Excel file names
xls_list = []
for file_name in os.listdir(path):
    if file_name.endswith('.xls'):
        xls_list.append(os.path.join(path, file_name))

# Lists for the station, parameter and observation DFs
station_df_list = []
param_df_list = []
obs_df_list = []

# Loop over the Excel files, convert worksheets into DFs and append them to the corresponding lists
for xls_name in xls_list:
    xls_dict = pd.read_excel(xls_name, sheet_name=None)
    for key in xls_dict.keys():
        sheet_name = key
        if sheet_name == 'Station_Metadata':
            station_df_list.append(xls_dict[sheet_name])
        elif sheet_name == 'Parameter_Metadata':
            param_df_list.append(xls_dict[sheet_name])
        else:
            if sheet_name != 'Methods_Metadata':
                obs_df_list.append(xls_dict[sheet_name])

# Create DFs of water quality stations, parameters and observations
print('DF of water quality stations:')
station_df = create_df(station_df_list)
print('DF of water quality parameters:')
param_df = create_df(param_df_list)
print('DF of water quality observations:')
obs_df = create_df(obs_df_list)

# Merge the DFs
gemstat_df = station_df.merge(obs_df, on='GEMS Station Number').merge(param_df, on='Parameter Code')
print('Merged DF:' + '\n')
print(gemstat_df.dtypes)

# Extract stations with location information
gemstat_df = gemstat_df[(gemstat_df['Latitude'].notnull()) & (gemstat_df['Longitude'].notnull())]

# Extract river stations
gemstat_df = gemstat_df[gemstat_df['Water Type'] == 'River station']

# Create a dictionary of parameters to be extracted from the DF
file_path = 'C:/Users/Holger/PycharmProjects/water-quality-modeling/data/params_to_extract.csv'
param_dict = pd.read_csv(file_path, sep=';').reset_index().to_dict(orient='list')

# Extract GEMStat parameters used in the study
gemstat_df = gemstat_df[gemstat_df['Parameter Code'].isin(param_dict['GEMStat'])]

# Exclude missing observation values
gemstat_df = gemstat_df[gemstat_df['Value'].notnull()]

# Keep only rows with positive values
gemstat_df = gemstat_df[gemstat_df['Value'] > 0]

# Exclude observation values that are estimated (~) and below (<) or above (>) detection limit
gemstat_df = gemstat_df[gemstat_df['Value Flags'].isnull()]

# Convert the sampling date into DateTime
gemstat_df['date'] = pd.to_datetime(gemstat_df['Sample Date'], format='%Y-%m-%d')

# Create a new DF with proper column names and write into a CSV
out_df = pd.DataFrame(
    {
        'lat': gemstat_df['Latitude'],
        'lon': gemstat_df['Longitude'],
        'date': gemstat_df['date'],
        'station_id': gemstat_df['GEMS Station Number'],
        'param_code': gemstat_df['Parameter Code'],
        'param_name': gemstat_df['Parameter Long Name'],
        'value': gemstat_df['Value'],
        'unit': gemstat_df['Unit'],
        'origin': 'GEMStat'
    }
)
out_df.to_csv(os.path.join(path, 'gemstat.csv'), sep=';', index=False)

# Extract the units of parameters and write into a CSV
unit_df = out_df.groupby(['param_code', 'param_name'])['unit'].unique().reset_index()
unit_df['origin'] = 'GEMStat'
unit_df.to_csv(os.path.join(path, 'gemstat_units.csv'), sep=';', index=False)
