from pandas import read_csv

census_city_populations_2010_filename = 'PEP_2017_PEPANNRSIP.US12A_with_ann.csv'
pd_city_populations = read_csv(census_city_populations_2010_filename,
                               encoding='ISO-8859-1',
                               header=1)
pd_city_populations = pd_city_populations.add_suffix(' census_2017')

census_city_areas_2010_filename = 'DEC_10_SF1_GCTPH1.US13PR_with_ann.csv'
pd_city_areas = read_csv(census_city_areas_2010_filename,
                         encoding='ISO-8859-1',
                         header=1)
pd_city_areas = pd_city_areas.add_suffix(' census_2010')

pd_combined_data = pd_city_populations.merge(
    pd_city_areas,
    left_on='Target Geo Id2 census_2017',
    right_on='Target Geo Id2 census_2010',
    how='inner')

pd_combined_data.to_csv('census_combined_data.csv', index=False)
