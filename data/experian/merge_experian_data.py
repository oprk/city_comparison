from pandas import read_csv

cities_worst_credit_score_filename = '500_cities_worst_credit_score.csv'
cities_worst_credit_score = read_csv(cities_worst_credit_score_filename,
                               encoding='ISO-8859-1')

cities_worst_credit_score = cities_worst_credit_score.drop('Rank', 1)

cities_best_credit_score_filename = '500_cities_best_credit_score.csv'
cities_best_credit_score = read_csv(cities_best_credit_score_filename,
                               encoding='ISO-8859-1')
cities_best_credit_score = cities_best_credit_score.drop('Rank', 1)

biggest_cities_best_credit_score_filename = '100_biggest_cities_best_credit_score.csv'
biggest_cities_best_credit_score = read_csv(biggest_cities_best_credit_score_filename,
                               encoding='ISO-8859-1')
biggest_cities_best_credit_score = biggest_cities_best_credit_score.drop('Rank', 1)

cities_credit_score_best_and_worst = cities_worst_credit_score.merge(
    cities_best_credit_score,
    on=['City', 'State', 'Credit Score'],
    how='outer')

all_cities_credit_scores = cities_credit_score_best_and_worst.merge(
    biggest_cities_best_credit_score,
    on=['City', 'State', 'Credit Score'],
    how='outer')

all_cities_credit_scores.to_csv('experian_combined_data.csv', index=False)
