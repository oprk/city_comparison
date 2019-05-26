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
    key = self.__class__.get_exact_matching_key()
    data = self.data.merge(data_table.data,
                           on=key,
                           how='inner',
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
    # We assume the state names match identically.
    if key1.state < key2.state:
      return -1
    if key1.state > key2.state:
      return 1
    # States are equal.  Now match cities.  Is one city name prefix of the
    # other?
    if (key1.city.startswith(key2.city)) or (key2.city.startswith(key1.city)):
      # Might be the same city.
      # Sanity check that populations are within 5% of each other.
      if (abs(key1.population - key2.population) / key2.population) > 0.1:
        print('Population too different (> 10%) to be the same city, continue:',
              key1, key2)
        # Probably just a coincidence that the cities begin with the same name,
        # if the populations are off by that much.
        if key1.city < key2.city:
          return -1
        else:
          return 1
      # Cities are probably a match because they are in same state, begin with
      # the same prefix, have about the same population.
      return 0
    if key1.city < key2.city:
      return -1
    if key1.city > key2.city:
      return 1
    return 0

  def join_fuzzy_matching(self, data_table):
    """Join with another DataTable of different type using fuzzy matching.

    We perform an 'inner' join, so rows that do not match will not be returned.

    Args:
      data_table: DataTable.

    Returns:
      DataTable of same class as left hand table.
    """
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
        # merged_result = merged_result.append(row_a, sort=True)
        i_a += 1
      elif compare > 0:
        # row_b is too small to match row_a:
        # merged_result = merged_result.append(row_b, sort=True)
        i_b += 1
      else:
        # By joining `on=None`, we join on `index`, which is 0 for both `row_a`
        # and `row_b`.  If `row_a` and `row_b` have duplicate columns, suffixes
        # will be added.
        merge_rows = row_a.join(row_b,
                                on=None,
                                how='inner',
                                lsuffix=self.suffix,
                                rsuffix=data_table.suffix,
                                sort=False)
        merged_result = merged_result.append(merge_rows,
                                             ignore_index=True,
                                             sort=True)
        i_a += 1
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
    data = pandas.read_excel(file_path, header=3)
    # Remove empty columns.
    data.drop(data.columns[[13, 14, 15, 16, 17, 18]], axis=1, inplace=True)

    # Replace the '\n' in header names and make lower_case:
    # "Murder and\nnonnegligent\nmanslaughter" =>
    # "murder_and_nonnegligent_manslaughter"
    def normalize_header(header):
      return header.lower().replace('\n', ' ')

    data = data.rename(columns=normalize_header)

    # Remove integers from 'city' and 'state' column values.  Also make
    # everything lowercase.
    def remove_integers(str_val):
      if isinstance(str_val, str):
        return ''.join([i for i in str_val if not i.isdigit()]).lower()
      else:
        return str_val

    def remove_integers_from_row(row):
      return pandas.Series(
          [remove_integers(row['city']),
           remove_integers(row['state'])])

    data[['city', 'state']] = data.apply(remove_integers_from_row, axis=1)

    # Propagate 'state' column.
    state = None
    for i, row in data.iterrows():
      if (pandas.notnull(row['state'])):
        state = row['state']
      data.set_value(i, 'state', state)

    return data

  @staticmethod
  def get_exact_matching_key():
    # By returning `None` as key, we use `index` as key.
    # return None
    return 'index'

  @staticmethod
  def get_state_key():
    return 'state'

  @staticmethod
  def get_city_key():
    return 'city'

  @staticmethod
  def get_population_key():
    return 'population'


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
