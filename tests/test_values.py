from pandas import read_csv

cc = read_csv('cities_comparison.csv', encoding='ISO-8859-1')

def test_nyc_area():
    nyc_row = cc.loc[cc['city'] == 'new york']
    print(nyc_row)
    nyc_area = nyc_row.get('Area in square miles - Land area census_2010')[0]
    assert nyc_area == 302.64
