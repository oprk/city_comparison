"""
Read in raw census CSV data and write out processed (modified) CSV data.
"""

import csv

_CENSUS_CSV = 'data/census.gov/PEP_2017_PEPANNRSIP.US12A_with_ann.csv'
_OUTPUT_CSV = './output.csv'


def fetch_census_data():
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
      # Split the "city, state" name into two fields.
      city['city'], city['state'] = city['Geography'].split(', ')
      all_cities.append(city)
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

  all_cities = fetch_census_data()
  # Check that we successfully fetched the city data.
  assert all_cities
  # Write CSV with "city, state" separated into two fields.
  _write_dicts_to_csv(all_cities, _OUTPUT_CSV)


if __name__ == '__main__':
  main()
