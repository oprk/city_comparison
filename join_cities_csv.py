import join_cities as jc
import pandas


def main():
  """Join Census data with FBI data and write out CSV."""

  census_population_2017_table = jc.Census(
      file_path='data/census.gov/PEP_2017_PEPANNRSIP.US12A_with_ann.csv',
      suffix='')

  print('census_population_2017_table.data:\n',
        len(census_population_2017_table.data))

  census_geography_2010_table = jc.Census(
      file_path='data/census.gov/DEC_10_SF1_GCTPH1.US13PR_with_ann.csv',
      suffix='_census_geo')

  print('census_geography_2010_table.data:\n',
        len(census_geography_2010_table.data))

  combined_census_table = (
      census_population_2017_table.join(census_geography_2010_table))
  print('combined_census_table.data:\n', len(combined_census_table.data))
  # with pandas.option_context('display.max_rows', None, 'display.max_columns',
  #                            None):
  #   print(combined_census_table.data[:2])

  fbi_crime_table = jc.Fbi(
      file_path=
      'data/fbi.gov/Table_8_Offenses_Known_to_Law_Enforcement_by_State_by_City_2017.xls',
      suffix='_fbi_crime')
  print('fbi_crime_table.data: ', len(fbi_crime_table.data))
  # with pandas.option_context('display.max_rows', None, 'display.max_columns',
  #                            None):
  #   print(fbi_crime_table.data[:2])

  combined_table = combined_census_table.join(fbi_crime_table)
  print('combined_table.data: ', len(combined_table.data))
  # with pandas.option_context('display.max_rows', None, 'display.max_columns',
  #                            None):
  #   print('combined_table.data: ', combined_table.data[:10])


if __name__ == '__main__':
  main()
