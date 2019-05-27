"""
Helper functions to cleanup the headers from the various data sources.
This helps make the final product more human readable.
"""

HEADERS_CHANGE = {
  'census_2010': {
    'rename_columns': {
      'Geographic area.1': 'geography census_2010',
      'Area in square miles - Total area': 'total area sqmi census_2010',
      'Area in square miles - Water area': 'water area sqmi census_2010',
      'Area in square miles - Land area': 'land area sqmi census_2010'
    },
    'drop_columns': [
      'Id', 'Id2', 'Geography', 'Housing units', 'Population', 'Target Geo Id',
      'Geographic area', 'Density per square mile of land area - Housing units',
      'Density per square mile of land area - Population'
    ]
  },
  'census_2017': {
    'rename_columns': {
      'Population Estimate (as of July 1) - 2010': 'population census_2010',
      'Population Estimate (as of July 1) - 2017': 'population census_2017'
    },
    'drop_columns': [
      'Id', 'Id2', 'Geography', 'Target Geo Id', 'Rank',
      'April 1, 2010 - Census', 'April 1, 2010 - Estimates Base',
      'Population Estimate (as of July 1) - 2011',
      'Population Estimate (as of July 1) - 2012',
      'Population Estimate (as of July 1) - 2013',
      'Population Estimate (as of July 1) - 2014',
      'Population Estimate (as of July 1) - 2015',
      'Population Estimate (as of July 1) - 2016'
    ]
  },
  'final_csv': {
    'rename_columns': {},
    'drop_columns': ['Target Geo Id2', 'state_fbi_crime']
  }
}


def drop_headers(data_source, pandas_dataframe):
  """ Mutate the pandas_dataframe and drop the headers from HEADERS_CHANGE """
  for column_name in pandas_dataframe.columns:
    if column_name in HEADERS_CHANGE[data_source]['drop_columns']:
      pandas_dataframe.drop(column_name, axis=1, inplace=True)
  return pandas_dataframe


def rename_headers(data_source, pandas_dataframe):
  """ Mutate the pandas_dataframe and rename the headers from HEADERS_CHANGE """
  pandas_dataframe.rename(columns=HEADERS_CHANGE[data_source]['rename_columns'],
                          inplace=True)


def cleanup_headers(data_source, pandas_dataframe):
  """ Helper function to drop and rename headers from HEADERS_CHANGE """
  drop_headers(data_source, pandas_dataframe)
  rename_headers(data_source, pandas_dataframe)
