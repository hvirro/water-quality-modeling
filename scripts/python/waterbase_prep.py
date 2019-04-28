# Import the libraries
import pandas as pd
import os

# Location of the files
dirname = 'C:/Users/Holger/PycharmProjects/water-quality-modeling/data/waterbase'

# Create a DF with the water quality stations
stat_df = pd.read_csv(os.path.join(dirname, 'Waterbase_v2016_1_WISE4_MonitoringSite_DerivedData.csv'))
stat_df.drop_duplicates(inplace=True)
stat_df.reset_index(drop=True, inplace=True)
print(stat_df.head())
print(stat_df.columns)
print(str(len(stat_df)) + ' stations are in the dataset.')

# Extract the stations with location information
loc_filter = (stat_df['lon'].isnull()) & (stat_df['lat'].isnull())
stat_df = stat_df[~loc_filter]
print(str(len(stat_df)) + ' stations are in the dataset.')

# Print out the different station types
print(stat_df['waterBodyIdentifierScheme'].unique())

# Extract the water quality stations on surface water bodies and the columns necessary for merging with the DF of
# observations
stat_df = stat_df[stat_df['waterBodyIdentifierScheme'].str.contains('Surface', na=False)]
stat_df = stat_df[['monitoringSiteIdentifier', 'lon', 'lat']]
print(str(len(stat_df)) + ' stations are in the dataset.')

# Create a DF with the parameters of the water quality observations
param_df = pd.read_csv(os.path.join(dirname, 'ObservedProperty.csv'))
print(param_df.head())
print(param_df.columns)

# Extract only the necessary columns
param_df = param_df[['Label', 'Notation']]

# Create a DF of observation data
obs_df = pd.read_csv(os.path.join(dirname, 'Waterbase_v2016_1_T_WISE4_DisaggregatedData.csv'))
obs_df.drop_duplicates(inplace=True)
obs_df.reset_index(drop=True, inplace=True)
print(obs_df.head())
print(obs_df.columns)
print(obs_df.dtypes)

# Extract only observations made in river stations
obs_df = obs_df[obs_df['parameterWaterBodyCategory'] == 'RW']

# Convert the sampling date into DateTime
obs_df['date'] = pd.to_datetime(obs_df['phenomenonTimeSamplingDate'], format='%Y-%m-%d')

# Merge the other DFs with stat_df
stat_df = stat_df.merge(obs_df, on='monitoringSiteIdentifier')
stat_df = stat_df.merge(param_df, left_on='observedPropertyDeterminandCode', right_on='Notation')

# Print out the final number of stations
print(str(len(stat_df['monitoringSiteIdentifier'].unique())) + ' stations remain in the dataset.')

# Check if there are missing observation values
print(str(stat_df['resultObservedValue'].isnull().sum()) + ' missing observation values are in the dataset.')

# Extract only rows that have observation values
stat_df = stat_df[pd.notnull(stat_df['resultObservedValue'])]
print(str(stat_df['resultObservedValue'].isnull().sum()) + ' missing observation values remain in the dataset.')

# Create a new DF with proper column names and write into a CSV
out_df = pd.DataFrame(
    {
        'lat': stat_df['lat'],
        'lon': stat_df['lon'],
        'date': stat_df['date'],
        'station_id': stat_df['monitoringSiteIdentifier'],
        'param_code': stat_df['observedPropertyDeterminandCode'],
        'param_desc': stat_df['Label'],
        'value': stat_df['resultObservedValue'],
        'unit': stat_df['resultUom'],
        'origin': 'Waterbase'
    }
)
out_df.to_csv(os.path.join(dirname, 'waterbase.csv'), sep=';', index=False)

# Extract the units of parameters and write into a CSV
unit_df = out_df.groupby('param_code')['unit'].unique().reset_index()
unit_df = unit_df.merge(param_df, left_on='param_code', right_on='Notation')
unit_df.drop(['Notation'], axis=1, inplace=True)
unit_df.to_csv(os.path.join(dirname, 'waterbase_units.csv'), sep=';', index=False)
