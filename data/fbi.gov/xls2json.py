"""
The files from https://ucr.fbi.gov/crime-in-the-u.s/2018/preliminary-report/cius-2018-preliminary-excel-tables-final.zip
come as binary xls files (unfortunately). This converts the xls to json.
"""

import collections
import json
import xlrd

FBI_DICT = {}


def convert_xls_to_json(xls_file):
  """ Convert one xls file to json """
  book = xlrd.open_workbook(xls_file)
  sheet_0 = book.sheet_by_index(0)
  print('Converting file: {} from xls to json.'.format(xls_file))
  if len(book.sheets()) > 1:
    print('More than one sheet in xls file, I am not prepared for this')
    exit(1)

  column_headers = []
  state_name = ''
  for rownum in range(sheet_0.nrows):
    row_values = sheet_0.row_values(rownum)
    # save the header information
    if rownum == 3:
      headers = [x.replace('\n', ' ') for x in row_values]
      column_headers = headers
    # if the population float or int is there, save that row to the dictionary
    if isinstance(row_values[2], float):
      xls_dict_row = collections.OrderedDict()
      for index, cell_value in enumerate(row_values):
        xls_dict_row[column_headers[index]] = cell_value

      if rownum > 3 and row_values[0] != '':
        state_name = row_values[0].lower()
      city_name = row_values[1].lower()
      city_name = ''.join([char for char in city_name if not char.isdigit()])
      # FBI_DICT[int(row_values[2])] = {'xls_dict_row': xls_dict_row}
      state_city = '{}_{}'.format(state_name, city_name)
      FBI_DICT[state_city] = {
          'xls_dict_row': xls_dict_row,
          'population': int(row_values[2]),
      }
    sheet_0.row_values(rownum)

  with open('fbi_cities_crime_2017.json', 'w') as file_handler:
    json.dump(FBI_DICT, file_handler)


convert_xls_to_json(
    'Table_8_Offenses_Known_to_Law_Enforcement_by_State_by_City_2017.xls'
)
