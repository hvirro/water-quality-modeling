# Import the libraries
import zipfile
import pandas as pd
import os

# Define a function for creating a DF from the sheet of an Excel file
def sheet_df(zipfiles, sheet_name):
    # Create a list of the sheets
    sheet_list = []
    for fname in zipfiles:
        zf = zipfile.ZipFile(fname, 'r')
        xls = zf.open(zf.namelist()[0])
        # Need to install the xlrd (pip install xlrd) library for the environment to use the read_excel() function
        df = pd.read_excel(xls, sheet_name=sheet_name, header=0)
        sheet_list.append(df)
    # Concatenate the files, drop duplicates and reset the index
    sheet_df = pd.concat(sheet_list)
    sheet_df.drop_duplicates(inplace=True)
    sheet_df.reset_index(drop=True, inplace=True)
    # Return the DF
    return sheet_df

# Location of the zipped Excel files
dirname = 'C:/Users/Holger/PycharmProjects/water-quality-modeling/data/gemstat'

# Create a list of the zipped Excel files
zipfiles = []
for f in os.listdir(dirname):
    if f.endswith('.zip'):
        zipfiles.append(os.path.join(dirname, f))

# Create a DF of the sheets with the stations
stat_df = sheet_df(zipfiles, 'Station_Metadata')
print(stat_df.head())
print(stat_df.columns)
print(str(len(stat_df)) + ' stations are in the dataset.')

# Print out the different station types
print(stat_df['Water Type'].unique())

# Extract only river stations and the columns necessary for merging with the DF of observations
stat_df = stat_df[stat_df['Water Type'] == 'River station']
stat_df = stat_df[['GEMS Station Number', 'Latitude', 'Longitude']]
print(str(len(stat_df)) + ' stations are in the dataset.')

# Create a DF of the sheets with the parameters
param_df = sheet_df(zipfiles, 'Parameter_Metadata')
print(param_df.head())
print(param_df.columns)

# Extract only the necessary columns
param_df = param_df[['Parameter Code', 'Parameter Long Name']]

# Create a DF of observation data
obs_df = pd.DataFrame(
    columns=['GEMS Station Number',
             'Sample Date',
             'Sample Time',
             'Depth',
             'Parameter Code',
             'Analysis Method Code',
             'Value Flags',
             'Value',
             'Unit',
             'Data Quality']
)

# Fill the DF with data
for fname in zipfiles:
    zf = zipfile.ZipFile(fname, 'r')
    xls_name = zf.namelist()[0]
    xls = pd.ExcelFile(zf.open(xls_name))
    obs_names = xls.sheet_names[3:]
    print('Filling the DF with data')
    print('Starting with {}'.format(xls_name))
    # Iterate over the sheets in the list
    for obs_sheet in obs_names:
        print('Loading table {}'.format(obs_sheet))
        # Convert the sheet into a DF
        sheet_df = pd.read_excel(xls, sheet_name=obs_sheet, header=0)
        # Concatenate the DFs
        obs_df = pd.concat([obs_df, sheet_df], ignore_index=False)
        obs_df.drop_duplicates(inplace=True)
        obs_df.reset_index(drop=True, inplace=True)
print(obs_df.head())
print(obs_df.columns)
print(obs_df.dtypes)

# Convert the sampling date into DateTime
obs_df['date'] = pd.to_datetime(obs_df['Sample Date'], format='%Y-%m-%d')

# Merge the other DFs with stat_df
stat_df = stat_df.merge(obs_df, on='GEMS Station Number')
stat_df = stat_df.merge(param_df, on='Parameter Code')

# Print out the final number of stations
print(str(len(stat_df['GEMS Station Number'].unique())) + ' stations remain in the dataset.')

# Check if there are missing observation values
print(str(stat_df['Value'].isnull().sum()) + ' missing observation values are in the dataset.')

# Extract only rows that have observation values
stat_df = stat_df[pd.notnull(stat_df['Value'])]
print(str(stat_df['Value'].isnull().sum()) + ' missing observation values remain in the dataset.')

# Create a new DF with proper column names and write into a CSV
out_df = pd.DataFrame(
    {
        'lat': stat_df['Latitude'],
        'lon': stat_df['Longitude'],
        'date': stat_df['date'],
        'station_id': stat_df['GEMS Station Number'],
        'param_code': stat_df['Parameter Code'],
        'param_desc': stat_df['Parameter Long Name'],
        'value': stat_df['Value'],
        'unit': stat_df['Unit'],
        'origin': 'GEMStat'
    }
)
out_df.to_csv(os.path.join(dirname, 'gemstat.csv'), sep=';', index=False)

# Extract the units of parameters and write into a CSV
unit_df = out_df.groupby('param_code')['unit'].unique().reset_index()
unit_df = unit_df.merge(param_df, left_on='param_code', right_on='Parameter Code')
unit_df.drop(['Parameter Code'], axis=1, inplace=True)
unit_df.to_csv(os.path.join(dirname, 'gemstat_units.csv'), sep=';', index=False)
