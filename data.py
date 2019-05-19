"""
Read in raw census CSV data and write out processed (modified) CSV data.
"""

import collections
import csv
import json

_CENSUS_CSV = 'data/census.gov/PEP_2017_PEPANNRSIP.US12A_with_ann.csv'
_OUTPUT_CSV = './cities_comparison.csv'
_CENSUS_HEADERS_KEPT = ['April 1, 2010 - Census', 'Population Estimate (as of July 1) - 2017']
_FBI_HEADERS_KEPT = ['Murder']

def get_fbi_dict_constant():
  with open('data/fbi.gov/fbi_cities_crime_2017.json', 'r') as filehandler:
    return json.loads(filehandler.read())

def get_kept_headers(headers_kept, city):
  row = collections.OrderedDict()
  for column_header in city:
    if column_header in headers_kept:
      row[column_header] = str(city[column_header])
  return row

def get_cleaned_census_row(city):
  # Split the "city, state" name into two fields.
  row = collections.OrderedDict()
  row['city'], row['state'] = city['Geography'].lower().split(', ')
  row['city'] = row['city'].strip(' city')

  return {**row, **get_kept_headers(_CENSUS_HEADERS_KEPT, city)}

def get_city_population_from_row(row):
  return int(dict(row)['Population Estimate (as of July 1) - 2017'])

def get_city_name(row):
  return dict(row)['city']

def get_fbi_row_on_city(city, fbi_dict):
  census_population = get_city_population_from_row(city)
  city_name = get_city_name(city)
  # note, if cities have the same population size, there will be a collision
  if census_population in fbi_dict:
    return get_kept_headers(_FBI_HEADERS_KEPT, fbi_dict[city_name]['csv_dict_row'])
  elif city_name in fbi_dict:
    fbi_population = fbi_dict[city_name]['population']
    population_discrepancy = abs(fbi_population - census_population)
    # if the city name matches, and the population is within 2 percent
    # I assume it's the same city.
    if population_discrepancy / census_population < 0.02:
      return get_kept_headers(_FBI_HEADERS_KEPT, fbi_dict[city_name]['csv_dict_row'])
    print('{} city population discrepenacy between fbi and census is too high'.format(city_name))
  print('city not found in census data: {}'.format(city_name))
  row = collections.OrderedDict()
  for header in _FBI_HEADERS_KEPT:
    row[header] = ''
  return row

def get_aggregated_csv_data():
  """
  Fetch all cities from census data,

  Returns:
    List of cities (dictionaries).
  """
  all_cities = []
  fbi_dict = get_fbi_dict_constant()
  with open(_CENSUS_CSV) as csv_file:
    # Skip the first line of the census data, because we know it is junk, and
    # the second line is actually the header we care about.
    next(csv_file)
    reader = csv.DictReader(csv_file, delimiter=',', quotechar='"')
    for city in reader:
      # clean up census csv, only keep the columns from: _CENSUS_HEADERS_KEPT
      cleaned_census_row = get_cleaned_census_row(city)
      # add the FBI crime data to the row for the city when available
      fbi_row_on_city = get_fbi_row_on_city(cleaned_census_row, fbi_dict)
      if fbi_row_on_city:
        all_cities.append({**cleaned_census_row, **fbi_row_on_city})

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
