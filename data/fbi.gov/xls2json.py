"""
The files from https://ucr.fbi.gov/crime-in-the-u.s/2018/preliminary-report/cius-2018-preliminary-excel-tables-final.zip
come as binary xls files (unfortunately). This converts them to csv.
"""

import collections
import json
import xlrd

fbi_dict = {}

def convert_xls_to_json(xls_file):
  """ Convert one xls file to a csv file. """
  book = xlrd.open_workbook(xls_file)
  sheet_0 = book.sheet_by_index(0)
  print('Converting file: {} from xls to json.'.format(xls_file))
  if len(book.sheets()) > 1:
    print('More than one sheet in xls file, I am not prepared for this')
    exit(1)

  column_headers = []

  for rownum in range(sheet_0.nrows):
    row_values = sheet_0.row_values(rownum)
    # save the header information
    if rownum == 4:
      headers = new_items = [x.replace('\n', '') for x in row_values]
      column_headers = headers
    # if the population float or int is there, save that row to the dictionary
    if isinstance(row_values[3], float):
      csv_dict_row = collections.OrderedDict()
      for index, cell_value in enumerate(row_values):
        csv_dict_row[column_headers[index]] = cell_value

      fbi_dict[int(row_values[3])] = {
        'city': row_values[1].lower(),
        'csv_dict_row': csv_dict_row
      }
    sheet_0.row_values(rownum)

  with open('fbi_cities_crim_2017.json', 'w') as fp:
    json.dump(fbi_dict, fp)


convert_xls_to_json('Table_4_January_to_June_2018_Offenses_Reported_to_Law_Enforcement_by_State_by_City_100,000_and_Over_in_Population.xls')
