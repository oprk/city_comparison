"""
Read in raw census CSV data and write out processed (modified) CSV data.
"""

import collections
import csv

_CENSUS_CSV = 'data/census.gov/PEP_2017_PEPANNRSIP.US12A_with_ann.csv'
_OUTPUT_CSV = './cities_comparison.csv'
_CENSUS_HEADERS_KEPT = ['April 1, 2010 - Census', 'Population Estimate (as of July 1) - 2017']


def get_cleaned_census_row(city):
  # Split the "city, state" name into two fields.
  row = collections.OrderedDict()
  row['city'], row['state'] = city['Geography'].split(', ')

  for column_header in city:
      if column_header in _CENSUS_HEADERS_KEPT:
          row[column_header] = city[column_header]

  return row

def get_city_population_from_row(row):
    return dict(row)['Population Estimate (as of July 1) - 2017']

def get_fbi_row_on_city(city):
    city_population_2017 = get_city_population_from_row(city)
    import ipdb; ipdb.set_trace()

def get_aggregated_csv_data():
  """
  Fetch all cities from census data,

  Returns:
    List of cities (dictionaries).
  """
  all_cities = []
  with open(_CENSUS_CSV) as csv_file:
    # Skip the first line of the census data, because we know it is junk, and
    # the second line is actually the header we care about.
    next(csv_file)
    reader = csv.DictReader(csv_file, delimiter=',', quotechar='"')
    for city in reader:
      # clean up census csv, only keep the columns from: _CENSUS_HEADERS_KEPT
      all_cities.append(get_cleaned_census_row(city))
      all_cities.append(get_fbi_row_on_city(city))
  return all_cities


def _write_dicts_to_csv(dict_list, path):
  """
  Write contents of `dict_list` as CSV to `path`.

  Args:
    dict_list: List of dictionaries.  All dictionaries in list should have the
      same keys.
    path: String path to output file to write CSV data to.
  """
  # If there is nothing to write, skip.  No output file will be created.
  if not dict_list:
    return
  field_names = dict_list[0].keys()
  with open(path, 'w') as csv_file:
    writer = csv.DictWriter(csv_file, field_names)
    writer.writeheader()
    for dictionary in dict_list:
      writer.writerow(dictionary)


def main():
  """
  Read in raw census CSV data and write out processed (modified) CSV data.
  """

  all_cities = get_aggregated_csv_data()
  # Check that we successfully fetched the city data.
  assert all_cities
  # Write CSV with "city, state" separated into two fields.
  _write_dicts_to_csv(all_cities, _OUTPUT_CSV)


if __name__ == '__main__':
  main()
