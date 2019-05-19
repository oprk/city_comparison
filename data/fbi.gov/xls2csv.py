"""
The files from https://ucr.fbi.gov/crime-in-the-u.s/2018/preliminary-report/cius-2018-preliminary-excel-tables-final.zip
come as binary xls files (unfortunately). This converts them to csv.
"""

import csv
import glob
import xlrd


def convert_xls_to_csv(xls_file):
  """ Convert one xls file to a csv file. """
  book = xlrd.open_workbook(xls_file)
  sheet_0 = book.sheet_by_index(0)
  print('Converting file: {} from xls to csv.'.format(xls_file))
  if len(book.sheets()) > 1:
    print('More than one sheet in xls file, I am not prepared for this')
    exit(1)
  csv_file_name = xls_file.replace('.xls', '.csv')

  with open(csv_file_name, 'w') as csv_file_handler:
    csv_writer = csv.writer(csv_file_handler,
                            dialect='excel',
                            quoting=csv.QUOTE_ALL)
    for rownum in range(sheet_0.nrows):
      csv_writer.writerow(sheet_0.row_values(rownum))


def get_xls_file_names(directory):
  """ Get a list of all the xls files. """
  return glob.glob('{}/*.xls'.format(directory))


xls_files = get_xls_file_names('.')

for xls_file in xls_files:
  convert_xls_to_csv(xls_file)
