import join_cities as jc
import pandas
import unittest


class TestFbi(unittest.TestCase):

  def test_get_exact_matching_key(self):
    self.assertEqual(jc.Fbi.get_exact_matching_key(), None)

  def test_get_state_key(self):
    self.assertEqual(jc.Fbi.get_state_key(), 'State')

  def test_get_city_key(self):
    self.assertEqual(jc.Fbi.get_city_key(), 'City')

  def test_get_population_key(self):
    self.assertEqual(jc.Fbi.get_population_key(), 'Population')

  def test_get_fuzzy_matching_key(self):
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
    fbi_table = jc.Fbi(data=df)
    self.assertEqual(
        fbi_table.get_fuzzy_matching_key(df.iloc[0]),
        jc.FuzzyMatchingKey(state='CA', city='Sunnyvale', population=100))

  def test_compare_keys_equal(self):
    key1 = jc.FuzzyMatchingKey(state='CA',
                               city='Sunnyvale City',
                               population=100)
    key2 = jc.FuzzyMatchingKey(state='CA', city='Sunnyvale', population=102)
    self.assertEqual(jc.Fbi.compare_keys(key1, key2), 0)

  def test_compare_keys_equal_wrong_population(self):
    key1 = jc.FuzzyMatchingKey(state='CA',
                               city='Sunnyvale City',
                               population=100)
    key2 = jc.FuzzyMatchingKey(state='CA', city='Sunnyvale', population=110)
    with self.assertRaises(ValueError):
      jc.Fbi.compare_keys(key1, key2)

  def test_compare_keys_city_less_than(self):
    key1 = jc.FuzzyMatchingKey(state='CA', city='Mountain View', population=1)
    key2 = jc.FuzzyMatchingKey(state='CA', city='Sunnyvale', population=102)
    self.assertEqual(jc.Fbi.compare_keys(key1, key2), -1)

  def test_compare_keys_state_less_than(self):
    key1 = jc.FuzzyMatchingKey(state='AL', city='Montgomery', population=1)
    key2 = jc.FuzzyMatchingKey(state='CA', city='Sunnyvale', population=102)
    self.assertEqual(jc.Fbi.compare_keys(key1, key2), -1)

  def test_compare_keys_population_less_than(self):
    key1 = jc.FuzzyMatchingKey(state='CA', city='Mountain View', population=1)
    key2 = jc.FuzzyMatchingKey(state='CA', city='Sunnyvale', population=102)
    self.assertEqual(jc.Fbi.compare_keys(key1, key2), -1)

  def test_read(self):
    df = jc.Fbi.read('data/fbi.gov/fbi_cities_crime_2017.json')
    # JSON file should contain 9577 rows.
    self.assertEqual(len(df), 9577)

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
    fbi_table = jc.Fbi(data=df)
    self.assertTrue(fbi_table.data.equals(df))

  def test_init_from_file(self):
    fbi_table = jc.Fbi(file_path='data/fbi.gov/fbi_cities_crime_2017.json')
    self.assertEqual(len(fbi_table.data), 9577)

  def test_join_exact_matching(self):
    fbi_table1 = jc.Fbi(data=pandas.DataFrame(
        {
            'foo': [1, 2],
            'State': ['CA', 'AL'],
            'City': ['Sunnyvale', 'Montgomery'],
            'Population': [100, 200],
        },
        index=['california_sunnyvale', 'alabama_montgomery']))
    fbi_table2 = jc.Fbi(data=pandas.DataFrame(
        {
            'bar': [3, 4],
            'State': ['AL', 'CA'],
            'City': ['Montgomery', 'Sunnyvale'],
            'Population': [200, 100],
        },
        index=['alabama_montgomery', 'california_sunnyvale']))
    # Note that the indices,
    # index=['california_sunnyvale', 'alabama_montgomery']
    # will be ignored.
    expected_data = pandas.DataFrame({
        'foo': [1, 2],
        'bar': [4, 3],
        'State': ['CA', 'AL'],
        'City': ['Sunnyvale', 'Montgomery'],
        'Population': [100, 200],
    }).sort_index(axis=1)
    actual_data = fbi_table1.join_exact_matching(fbi_table2).data.sort_index(
        axis=1)
    # We sort the pandas DataFrame columns in order to compare.
    self.assertTrue(expected_data.equals(actual_data))


if __name__ == '__main__':
  unittest.main()
