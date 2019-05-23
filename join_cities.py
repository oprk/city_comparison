"""
Utilities to join FBI and Census data.
We'll generalize to support joining other data tables in the future.
"""

from abc import ABC, abstractmethod
import collections
import pandas

FuzzyMatchingKey = collections.namedtuple('FuzzyMatchingKey', ['state', 'city', 'population'])


class DataTable(ABC):

  def __init__(self, data=None, file_path=None, suffix=''):
    """
    Create a DataTable containing rows of city data.

    Args:
      data: (Optional) Pandas dataframe.
      file_path: (Optional String) data file path.
    """
    self._file_path = file_path
    self._suffix = suffix
    if data is not None:
      self._data = data
    else:
      assert self._file_path is not None
      self._data = self.__class__.read(self._file_path)

  @property
  def suffix(self):
    return self._suffix

  @property
  def data(self):
    return self._data

  @staticmethod
  @abstractmethod
  def read(file_path):
    pass

  @staticmethod
  @abstractmethod
  def get_exact_matching_key():
    pass

  def join_exact_matching(self, data_table):
    data = self._data.merge(
      data_table.data, on=self.__class__.get_exact_matching_key(), how='outer',
      suffixes=[self.suffix, data_table.suffix])
    return self.__class__(data)

  @staticmethod
  @abstractmethod
  def get_state_key():
    pass

  @staticmethod
  @abstractmethod
  def get_city_key():
    pass

  @staticmethod
  @abstractmethod
  def get_population_key():
    pass

  @classmethod
  def get_fuzzy_matching_key(cls, row):
    """Extract key that uniquely identifies row of city data.

    Args:
      row: Pandas dataframe row, data for one city.

    Returns:
      FuzzyMatchingKey.
    """
    return FuzzyMatchingKey(
      state=row[cls.get_state_key()],
      city=row[cls.get_city_key()],
      population=row[cls.get_population_key()])

  @staticmethod
  def compare_keys(key1, key2):
    """Comparison function for keys.

    Perform fuzzy matching on keys.

    Returns:
      -1 if key1 < key2
      0 if key1 == key2
      1 if key1 > key2
    """
    if key1.state < key2.state:
      return -1
    elif key1.state > key2.state:
      return 1
    else:
      # States are equal.  Now match cities.  Is one city name prefix of the
      # other?
      if (key1.city.startswith(key2.city)) or (key2.city.startswith(key1.city)):
        # Consider cities equal.
        # Sanity check that populations are within 5% of each other.
        if (abs(key1.population - key2.population) / key2.population) > 0.05:
          raise ValueError('Population measurements of the same city should be within 5% of each other.')
        return 0
      if key1.city < key2.city:
        return -1
      elif key1.city > key2.city:
        return 1

  def join_fuzzy_matching(self, data_table):
    keys_a = [self.get_state_key(),
              self.get_city_key(),
              self.get_population_key()]
    keys_a = [data_table.get_state_key(),
              data_table.get_city_key(),
              data_table.get_population_key()]
    rows_a = data_table.sort_values(by=keys_a)
    rows_b = self._data.sort_values(by=keys_b)
    i_a = 0
    i_b = 0
    merged_result = pandas.DataFrame()
    while i_a < len(rows_a) and i_b < len(rows_b):
      row_a = rows_a.iloc[i_a, :]
      row_b = rows_b.iloc[i_b, :]
      p = DataTable.compare_keys(get_fuzzy_matching_key(row_a),
                                 get_fuzzy_matching_key(row_b))
      if p < 0:
        # row_a is too small to match row_b.
        merged_result.append(row_a)
        i_a += 1
      elif p > 0:
        # row_b is too small to match row_a:
        merged_result.append(row_b)
        i_b += 1
      else:
        # Keys match.
        merged_result.append(
          row_a.join(row_b, lsuffix=self.suffix, rsuffix=data_table.suffix))
        i_a += 1
        i_b += 1
    return self._class__(merged_result)

  def join(self, data_table):
    # If same class, join exact.
    if isinstance(data_table, self.__class__):
      return self.join_exact_matching(data_table)
    else:
      return self.join_fuzzy_matching(data_table)


class Fbi(DataTable):

  def read(file_path):
    return pandas.read_json(file_path, encoding='ISO-8859-1', orient='index')

  def get_exact_matching_key():
    # By returning `None` as key, we use `index` as key.
    return None

  def get_state_key():
    return 'State'

  def get_city_key():
    return 'City'

  def get_population_key():
    return 'Population'


class Census(DataTable):

  def read(self, file_path):
    """Census data is stored as CSV.

    Args:
      file_path: String path to file.

    Returns:
      Pandas dataframe.
    """
    return pandas.read_csv(file_path, encoding='ISO-8859-1')

  def get_exact_matching_key():
    return 'GC_RANK.target-geo-id2'

  def get_state_key():
    return 'state'

  def get_city_key():
    return 'city'

  def get_population_key():
    return 'Population Estimate (as of July 1) - 2017'
