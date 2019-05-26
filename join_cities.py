"""
Utilities to join FBI and Census data.
We'll generalize to support joining other data tables in the future.
"""

from abc import ABC, abstractmethod
import collections
import pandas

FuzzyMatchingKey = collections.namedtuple('FuzzyMatchingKey',
                                          ['state', 'city', 'population'])


class DataTable(ABC):
  """Data table where each row is statistics for a city."""

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
    """Suffix to apply to this table's fields when joining with other tables."""
    return self._suffix

  @property
  def data(self):
    """Data represented as pandas DataFrame."""
    return self._data

  @staticmethod
  @abstractmethod
  def read(file_path):
    """Read data from file and return as pandas DataFrame."""

  @staticmethod
  @abstractmethod
  def get_exact_matching_key():
    """Key to use for exact matching."""

  def join_exact_matching(self, data_table):
    """Join with another DataTable of the same type using exact matching."""
    data = self._data.merge(data_table.data,
                            on=self.__class__.get_exact_matching_key(),
                            how='outer',
                            suffixes=[self.suffix, data_table.suffix])
    return self.__class__(data)

  @staticmethod
  @abstractmethod
  def get_state_key():
    """Key for `state` name."""

  @staticmethod
  @abstractmethod
  def get_city_key():
    """Key for `city` name."""

  @staticmethod
  @abstractmethod
  def get_population_key():
    """Key for `population`."""

  @classmethod
  def get_fuzzy_matching_key(cls, row):
    """Extract key that uniquely identifies row of city data.

    Args:
      row: Pandas dataframe row, data for one city.

    Returns:
      FuzzyMatchingKey.
    """
    return FuzzyMatchingKey(state=row[cls.get_state_key()],
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
    if key1.state > key2.state:
      return 1
    # States are equal.  Now match cities.  Is one city name prefix of the
    # other?
    if (key1.city.startswith(key2.city)) or (key2.city.startswith(key1.city)):
      # Consider cities equal.
      # Sanity check that populations are within 5% of each other.
      if (abs(key1.population - key2.population) / key2.population) > 0.05:
        raise ValueError(
            'Population measurements of the same city should be within 5% of each other.'
        )
      return 0
    if key1.city < key2.city:
      return -1
    if key1.city > key2.city:
      return 1
    return 0

  def join_fuzzy_matching(self, data_table):
    """Join with another DataTable of different type using fuzzy matching."""
    keys_a = [
        self.get_state_key(),
        self.get_city_key(),
        self.get_population_key()
    ]
    keys_b = [
        data_table.get_state_key(),
        data_table.get_city_key(),
        data_table.get_population_key()
    ]
    rows_a = self._data.sort_values(by=keys_a)
    rows_b = data_table.data.sort_values(by=keys_b)
    rows_a.reset_index(inplace=True, drop=True)
    rows_b.reset_index(inplace=True, drop=True)
    i_a = 0
    i_b = 0
    merged_result = pandas.DataFrame()
    while i_a < len(rows_a) and i_b < len(rows_b):
      row_a = rows_a[i_a:i_a + 1]
      row_b = rows_b[i_b:i_b + 1]
      # Keys match.  Drop the indices in `row_a` and `row_b` so they both have
      # index `0` for concatenation.
      row_a.reset_index(inplace=True, drop=True)
      row_b.reset_index(inplace=True, drop=True)

      compare = DataTable.compare_keys(
          self.__class__.get_fuzzy_matching_key(row_a.iloc[0]),
          data_table.get_fuzzy_matching_key(row_b.iloc[0]))
      if compare < 0:
        # row_a is too small to match row_b.
        merged_result = merged_result.append(row_a, sort=True)
        i_a += 1
      elif compare > 0:
        # row_b is too small to match row_a:
        merged_result = merged_result.append(row_b, sort=True)
        i_b += 1
      else:
        merge_rows = pandas.concat([row_a, row_b], axis=1, sort=True)
        merged_result = merged_result.append(merge_rows,
                                             ignore_index=True,
                                             sort=True)
        i_a += 1
        i_b += 1

    # There may be some stragglers.
    while i_a < len(rows_a):
      row_a = rows_a[i_a:i_a + 1]
      row_a.reset_index(inplace=True, drop=True)
      merged_result = merged_result.append(row_a, sort=True)
      i_a += 1

    while i_b < len(rows_b):
      row_b = rows_b[i_b:i_b + 1]
      row_b.reset_index(inplace=True, drop=True)
      merged_result = merged_result.append(row_b, sort=True)
      i_b += 1

    # Drop the indices in `merged_result`, because they don't mean anything either.
    merged_result.reset_index(inplace=True, drop=True)
    # NaNs are difficult to deal with.  Replace with 0 instead.
    merged_result = merged_result.fillna(0)

    return self.__class__(merged_result)

  def join(self, data_table):
    """Join with another DataTable.

    Dispatches to use either "exact" or "fuzzy" matching based on whether
    DataTables are of the same type.

    Args:
      data_table: DataTable.

    Returns:
      DataTable.
    """

    # If same class, join exact.
    if isinstance(data_table, self.__class__):
      return self.join_exact_matching(data_table)
    return self.join_fuzzy_matching(data_table)


class Fbi(DataTable):
  """Table of FBI data."""

  @staticmethod
  def read(file_path):
    data = pandas.read_json(file_path, encoding='ISO-8859-1', orient='index')
    # Turn default index into a column named 'index'.
    data.reset_index(inplace=True)
    return data

  @staticmethod
  def get_exact_matching_key():
    # By returning `None` as key, we use `index` as key.
    # return None
    return 'index'

  @staticmethod
  def get_state_key():
    return 'State'

  @staticmethod
  def get_city_key():
    return 'City'

  @staticmethod
  def get_population_key():
    return 'Population'


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
    return pandas.read_csv(file_path, encoding='ISO-8859-1', header=1)

  @staticmethod
  def get_exact_matching_key():
    return 'Target Geo Id'

  @staticmethod
  def get_state_key():
    return 'state'

  @staticmethod
  def get_city_key():
    return 'city'

  @staticmethod
  def get_population_key():
    return 'Population Estimate (as of July 1) - 2017'
