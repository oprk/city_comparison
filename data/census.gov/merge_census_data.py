from pandas import read_csv

census_city_populations_2010_filename = 'PEP_2017_PEPANNRSIP.US12A_with_ann.csv'
pd_city_populations = read_csv(census_city_populations_2010_filename,
                               encoding='ISO-8859-1')

census_city_areas_2010_filename = 'DEC_10_SF1_GCTPH1.US13PR_with_ann.csv'
pd_city_areas = read_csv(census_city_areas_2010_filename, encoding='ISO-8859-1')

pd_city_areas.rename(
    columns={'GCT_STUB.target-geo-id2': 'GC_RANK.target-geo-id2'}, inplace=True)

pd_combined_data = pd_city_populations.merge(pd_city_areas,
                                             on='GC_RANK.target-geo-id2',
                                             how='outer')

pd_combined_data.to_csv('census_combined_data.csv', index=False)
