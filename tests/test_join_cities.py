from data_table_census import Census as census_data_table
from data_table_fbi import Fbi as fbi_data_table
from data_table import FuzzyMatchingKey

import pandas
import unittest


class TestFbi(unittest.TestCase):

  def test_get_exact_matching_key(self):
    self.assertEqual(fbi_data_table.get_exact_matching_key(), 'index')

  def test_get_state_key(self):
    self.assertEqual(fbi_data_table.get_state_key(), 'state')

  def test_get_city_key(self):
    self.assertEqual(fbi_data_table.get_city_key(), 'city')

  def test_get_population_key(self):
    self.assertEqual(fbi_data_table.get_population_key(), 'population')

  def test_get_fuzzy_matching_key(self):
    df = pandas.DataFrame(
        {
            'foo': 1,
            'state': 'CA',
            'city': 'Sunnyvale',
            'population': 100,
            'State': 'ignored',
            'City': 'ignored'
        },
        index=[0])
    fbi_table = fbi_data_table(data=df)
    self.assertEqual(
        fbi_table.get_fuzzy_matching_key(df.iloc[0]),
        FuzzyMatchingKey(state='CA', city='Sunnyvale', population=100))

  def test_compare_keys_equal(self):
    key1 = FuzzyMatchingKey(state='CA', city='Sunnyvale City', population=100)
    key2 = FuzzyMatchingKey(state='CA', city='Sunnyvale', population=102)
    self.assertEqual(fbi_data_table.compare_keys(key1, key2), 0)

  def test_compare_keys_equal_wrong_population(self):
    key1 = FuzzyMatchingKey(state='CA', city='Sunnyvale City', population=100)
    key2 = FuzzyMatchingKey(state='CA', city='Sunnyvale', population=200)
    self.assertEquals(fbi_data_table.compare_keys(key1, key2), 1)

  def test_compare_keys_city_less_than(self):
    key1 = FuzzyMatchingKey(state='CA', city='Mountain View', population=1)
    key2 = FuzzyMatchingKey(state='CA', city='Sunnyvale', population=102)
    self.assertEqual(fbi_data_table.compare_keys(key1, key2), -1)

  def test_compare_keys_state_less_than(self):
    key1 = FuzzyMatchingKey(state='AL', city='Montgomery', population=1)
    key2 = FuzzyMatchingKey(state='CA', city='Sunnyvale', population=102)
    self.assertEqual(fbi_data_table.compare_keys(key1, key2), -1)

  def test_compare_keys_population_less_than(self):
    key1 = FuzzyMatchingKey(state='CA', city='Mountain View', population=1)
    key2 = FuzzyMatchingKey(state='CA', city='Sunnyvale', population=102)
    self.assertEqual(fbi_data_table.compare_keys(key1, key2), -1)

  def test_read(self):
    df = fbi_data_table.read(
        'data/fbi/Table_8_Offenses_Known_to_Law_Enforcement_by_State_by_City_2017.xls'
    )
    self.assertEqual(len(df), 9589)

  def test_init_from_data(self):
    # Test initializing an `Fbi` DataTable from pandas dataframe.
    df = pandas.DataFrame(
        {
            'foo': 1,
            'State': 'CA',
            'City': 'Sunnyvale',
            'Population': 100,
            'state': 'ignored',
            'city': 'ignored'
        },
        index=[0])
    fbi_table = fbi_data_table(data=df)
    self.assertTrue(fbi_table.data.equals(df))

  def test_init_from_file(self):
    fbi_table = fbi_data_table(
        file_path=
        'data/fbi/Table_8_Offenses_Known_to_Law_Enforcement_by_State_by_City_2017.xls'
    )
    self.assertEqual(len(fbi_table.data), 9589)

  def test_join_exact_matching(self):
    fbi_data1 = pandas.DataFrame(
        {
            'index': ['california_sunnyvale', 'alabama_montgomery'],
            'foo': [1, 2],
            'State': ['CA', 'AL'],
            'City': ['Sunnyvale', 'Montgomery'],
            'Population': [100, 200],
        },
        index=[0, 1])

    fbi_data2 = pandas.DataFrame(
        {
            'index': ['alabama_montgomery', 'california_sunnyvale'],
            'bar': [3, 4],
            'State': ['AL', 'CA'],
            'City': ['Montgomery', 'Sunnyvale'],
            'Population': [200, 100],
        },
        index=[0, 1])

    fbi_table1 = fbi_data_table(data=fbi_data1, suffix='_table1')
    fbi_table2 = fbi_data_table(data=fbi_data2, suffix='_table2')
    # Note that the indices,
    # index=['california_sunnyvale', 'alabama_montgomery']
    # will be ignored.
    expected_data = pandas.DataFrame({
        'foo': [1, 2],
        'bar': [4, 3],
        'State_table1': ['CA', 'AL'],
        'City_table1': ['Sunnyvale', 'Montgomery'],
        'Population_table1': [100, 200],
        'State_table2': ['CA', 'AL'],
        'City_table2': ['Sunnyvale', 'Montgomery'],
        'Population_table2': [100, 200],
        'index': ['california_sunnyvale', 'alabama_montgomery']
    }).sort_index(axis=1)
    actual_data = fbi_table1.join_exact_matching(fbi_table2).data.sort_index(
        axis=1)
    # We sort the pandas DataFrame columns in order to compare
    self.assertTrue(expected_data.equals(actual_data))


class TestCensus(unittest.TestCase):

  def test_get_exact_matching_key(self):
    self.assertEqual(census_data_table.get_exact_matching_key(),
                     'Target Geo Id2')

  def test_get_state_key(self):
    self.assertEqual(census_data_table.get_state_key(), 'state')

  def test_get_city_key(self):
    self.assertEqual(census_data_table.get_city_key(), 'city')

  def test_get_population_key(self):
    self.assertEqual(census_data_table.get_population_key(),
                     'Population Estimate (as of July 1) - 2017')

  def test_get_fuzzy_matching_key(self):
    df = pandas.DataFrame(
        {
            'foo': 1,
            'state': 'CA',
            'city': 'Sunnyvale',
            'Population Estimate (as of July 1) - 2017': 100,
            'State': 'ignored',
            'City': 'ignored'
        },
        index=[0])
    census_table = census_data_table(data=df)
    self.assertEqual(
        census_table.get_fuzzy_matching_key(df.iloc[0]),
        FuzzyMatchingKey(state='CA', city='Sunnyvale', population=100))

  def test_read(self):
    df = census_data_table.read(
        'data/census/PEP_2017_PEPANNRSIP.US12A_with_ann.csv')
    self.assertEqual(len(df), 769)

  def test_init_from_data(self):
    # Test initializing an `Census` DataTable from pandas dataframe.
    df = pandas.DataFrame(
        {
            'foo': 1,
            'state': 'CA',
            'city': 'Sunnyvale',
            'Population Estimate (as of July 1) - 2017': 100,
            'State': 'ignored',
            'City': 'ignored'
        },
        index=[0])
    census_table = census_data_table(data=df)
    self.assertTrue(census_table.data.equals(df))

  def test_init_from_file(self):
    census_table = census_data_table(
        file_path='data/census/PEP_2017_PEPANNRSIP.US12A_with_ann.csv')
    self.assertEqual(len(census_table.data), 769)

  def test_join_exact_matching(self):
    census_data1 = pandas.DataFrame(
        {
            'foo': [1, 2],
            'state': ['CA', 'AL'],
            'city': ['Sunnyvale', 'Montgomery'],
            'Target Geo Id2': ['1620000US0677000', '1620000US0151000'],
            'Population Estimate (as of July 1) - 2017': [100, 200],
        },
        index=[0, 1])
    census_data2 = pandas.DataFrame(
        {
            'bar': [3, 4],
            'state': ['AL', 'CA'],
            'city': ['Montgomery', 'Sunnyvale'],
            'Target Geo Id2': ['1620000US0151000', '1620000US0677000'],
            'Population Estimate (as of July 1) - 2017': [200, 100],
        },
        index=[0, 1])
    census_table1 = census_data_table(data=census_data1, suffix='_table1')
    census_table2 = census_data_table(data=census_data2, suffix='_table2')
    # Notice that the overlapping columns are duplicated, with suffix '_table1'
    # and '_table2'.
    expected_data = pandas.DataFrame({
        'foo': [1, 2],
        'bar': [4, 3],
        'state_table1': ['CA', 'AL'],
        'state_table2': ['CA', 'AL'],
        'city_table1': ['Sunnyvale', 'Montgomery'],
        'city_table2': ['Sunnyvale', 'Montgomery'],
        'Target Geo Id2': ['1620000US0677000', '1620000US0151000'],
        'Population Estimate (as of July 1) - 2017_table1': [100, 200],
        'Population Estimate (as of July 1) - 2017_table2': [100, 200],
    }).sort_index(axis=1)
    actual_data = census_table1.join_exact_matching(
        census_table2).data.sort_index(axis=1)
    # We sort the pandas DataFrame columns in order to compare.
    self.assertTrue(expected_data.equals(actual_data))


class TestFuzzyMatching(unittest.TestCase):

  def test_join_fuzzy_matching(self):
    fbi_data1 = pandas.DataFrame(
        {
            'foo': [1, 2, 3],
            'state': ['CA', 'AL', 'Hidden'],
            'city': ['Sunnyvale', 'Montgomery', 'Lost City'],
            'population': [100, 200, 300],
            'index': [
                'california_sunnyvale', 'alabama_montgomery',
                'atlantas_lost_city'
            ],
        },
        index=['california_sunnyvale', 'alabama_montgomery', 'atlantis'])
    census_data2 = pandas.DataFrame(
        {
            'bar': [5, 3, 4],
            'state': ['Isle of Man', 'AL', 'CA'],
            'city': ['Avalon', 'Montgomery', 'Sunnyvale'],
            'Target Geo Id2': ['???', '1620000US0151000', '1620000US0677000'],
            'Population Estimate (as of July 1) - 2017': [1, 200, 100],
        },
        index=[0, 1, 2])

    fbi_table1 = fbi_data_table(data=fbi_data1, suffix='_fbi')
    census_table2 = census_data_table(data=census_data2, suffix='_census')
    joined_table = fbi_table1.join_fuzzy_matching(census_table2)
    # The output table from matching should be the same class as the left table.
    self.assertTrue(isinstance(joined_table, fbi_data_table))
    actual_data = joined_table.data.sort_index(axis=1)
    expected_data = pandas.DataFrame({
        'city_fbi': ['Montgomery', 'Sunnyvale'],
        'population': [200, 100],
        'Population Estimate (as of July 1) - 2017': [200, 100],
        'state_census': ['AL', 'CA'],
        'Target Geo Id2': ['1620000US0151000', '1620000US0677000'],
        'bar': [3, 4],
        'foo': [2, 1],
        'city_census': ['Montgomery', 'Sunnyvale'],
        'index': ['alabama_montgomery', 'california_sunnyvale'],
        'state_fbi': ['AL', 'CA'],
    }).sort_index(axis=1)

    self.assertTrue(expected_data.equals(actual_data))


if __name__ == '__main__':
  unittest.main()
