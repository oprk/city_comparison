"""
Module for parsing any Census related data in data/census.
"""
import pandas
from data_table import DataTable


class Census(DataTable):
  """Table of Census data."""

  @staticmethod
  def read(file_path):
    """Census data is stored as CSV.

    Args:
      file_path: String path to file.

    Returns:
      Pandas dataframe.
    """
    # header=1 skips line 0 and uses line 1 as the header.
    data = pandas.read_csv(file_path, encoding='ISO-8859-1', header=1)
    # Parse out 'state' and 'city' field from 'Geography.2' field.  There's a
    # '.2' because multiple fields in the header are called 'Geography'.  We
    # should clean that up sometime.
    if 'Geography.2' in data:

      def parse_city_and_state(row):
        city, state = row['Geography.2'].lower().split(', ')
        city = city.rstrip(' city')
        return pandas.Series([city, state])

      data[['city', 'state']] = data.apply(parse_city_and_state, axis=1)

    return data

  @staticmethod
  def get_exact_matching_key():
    return 'Target Geo Id2'

  @staticmethod
  def get_state_key():
    return 'state'

  @staticmethod
  def get_city_key():
    return 'city'

  @staticmethod
  def get_population_key():
    return 'Population Estimate (as of July 1) - 2017'
