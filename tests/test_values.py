"""
Tests as a sanity check to make sure the data matches what we expect.
"""

from pandas import read_csv

CC = read_csv('city_comparison.csv', encoding='ISO-8859-1')


def test_nyc_area():
  """ We know the 2010 census area for nyc is 302.64, let's test this. """
  nyc_row = CC.loc[CC['city'] == 'new york']
  print(nyc_row)
  nyc_area = nyc_row.get('Area in square miles - Land area')
  assert float(nyc_area) == 302.64
