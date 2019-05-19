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

  for rownum in range(sheet_0.nrows):
    row_values = sheet_0.row_values(rownum)
    # save the header information
    if rownum == 4:
      headers = [x.replace('\n', '') for x in row_values]
      column_headers = headers
    # if the population float or int is there, save that row to the dictionary
    if isinstance(row_values[3], float):
      xls_dict_row = collections.OrderedDict()
      for index, cell_value in enumerate(row_values):
        xls_dict_row[column_headers[index]] = cell_value

      FBI_DICT[int(row_values[3])] = {'xls_dict_row': xls_dict_row}
      FBI_DICT[row_values[1].lower()] = {
          'xls_dict_row': xls_dict_row,
          'population': int(row_values[3]),
      }
    sheet_0.row_values(rownum)

  with open('fbi_cities_crime_2017.json', 'w') as file_handler:
    json.dump(FBI_DICT, file_handler)


convert_xls_to_json(
    # pylint: disable=line-too-long
    'Table_4_January_to_June_2018_Offenses_Reported_to_Law_Enforcement_by_State_by_City_100,000_and_Over_in_Population.xls'
)
