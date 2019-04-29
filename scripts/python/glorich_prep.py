# Import the libraries
import geopandas as gpd
import os
import pandas as pd

# Location of the files
dirname = 'C:/Users/Holger/PycharmProjects/water-quality-modeling/data/glorich'

# Create a DF with the water quality stations
stat_df = gpd.read_file(os.path.join(dirname, 'Sampling_Locations_v1.shp'))
print(stat_df.head())
print(stat_df.columns)
print(str(len(stat_df)) + ' stations are in the dataset.')

# Add new columns with the latitude and longitude
stat_df['lat'] = stat_df['geometry'].y
stat_df['lon'] = stat_df['geometry'].x

# Extract only the columns necessary for merging with the DF of observations
stat_df = stat_df[['STAT_ID', 'lat', 'lon']]
print(stat_df.head())

# Create a DF with the parameters of the water quality observations
param_df = pd.read_csv(os.path.join(dirname, 'parameters.csv'), sep=';')
print(param_df.head())
print(param_df.columns)

# Create a DF of observation data
obs_df = pd.read_csv(os.path.join(dirname, 'hydrochemistry.csv'), sep=';', encoding='ISO-8859-1')
obs_df.drop_duplicates(inplace=True)
obs_df.reset_index(drop=True, inplace=True)
print(obs_df.head())
print(obs_df.columns)
print(obs_df.dtypes)

# Convert the sampling date into DateTime
obs_df['date'] = pd.to_datetime(obs_df['RESULT_DATETIME']).dt.strftime('%Y-%m-%d')
print(obs_df.dtypes)

# Create a list of columns with remarks about the observations
vrc_cols = []
for col in obs_df.columns:
    if 'vrc' in col:
        vrc_cols.append(col)
print(vrc_cols)

# Create a list of columns with values of the observations
value_cols = []
for col in vrc_cols:
    value_cols.append(col[:-4])
print(value_cols)

# Melt the parameters and the values of the observations into two columns
value_df = pd.melt(
    obs_df, id_vars=['STAT_ID', 'date'], value_vars=value_cols,
    var_name='obs_param', value_name='obs_value'
)
print(value_df.head())

# Convert values into the correct type
value_df['obs_value'] = value_df['obs_value'].str.replace(',', '.')
value_df['obs_value'] = value_df['obs_value'].astype(float)

# Melt the names and the values of the remarks into two columns
vrc_df = pd.melt(
    obs_df, id_vars=['STAT_ID'], value_vars=vrc_cols,
    var_name='remark_param', value_name='remark_value'
)
vrc_df.drop(['STAT_ID'], axis=1, inplace=True)
print(vrc_df.head())

# Concat the DFs
obs_df = pd.concat([value_df, vrc_df], axis=1)
print(obs_df.head())
print(obs_df.columns)

# Check if there are any remarks about the observations
print(str(obs_df['remark_value'].notnull().sum()) + ' remarks are in the dataset.')

# Extract only rows that do not have remarks
obs_df = obs_df[pd.isnull(obs_df['remark_value'])]
print(str(obs_df['remark_value'].notnull().sum()) + ' remarks remain in the dataset.')

# Merge the other DFs with stat_df
stat_df = stat_df.merge(obs_df, on='STAT_ID')
stat_df = stat_df.merge(param_df, left_on='obs_param', right_on='Parameter name')

# Print out the final number of stations
print(str(len(stat_df['STAT_ID'].unique())) + ' stations remain in the dataset.')

# Check if there are missing observation values
print(str(stat_df['obs_value'].isnull().sum()) + ' missing observation values are in the dataset.')

# Extract only rows that have observation values
stat_df = stat_df[pd.notnull(stat_df['obs_value'])]
print(str(stat_df['obs_value'].isnull().sum()) + ' missing observation values remain in the dataset.')

# Create a new DF with proper column names and write into a CSV
out_df = pd.DataFrame(
    {
        'lat': stat_df['lat'],
        'lon': stat_df['lon'],
        'date': stat_df['date'],
        'station_id': stat_df['STAT_ID'],
        'param_code': stat_df['obs_param'],
        'param_desc': stat_df['Description'],
        'value': stat_df['obs_value'],
        'unit': stat_df['Unit'],
        'origin': 'GLORICH'
    }
)
out_df.to_csv(os.path.join(dirname, 'glorich.csv'), sep=';', index=False)

# Extract the units of parameters and write into a CSV
unit_df = out_df.groupby('param_code')['unit'].unique().reset_index()
unit_df = unit_df.merge(param_df, left_on='param_code', right_on='Parameter name')
unit_df.drop(['Parameter name', 'Unit'], axis=1, inplace=True)
unit_df.to_csv(os.path.join(dirname, 'glorich_units.csv'), sep=';', index=False)
