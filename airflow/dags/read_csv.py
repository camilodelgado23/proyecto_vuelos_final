import pandas as pd
flights = pd.read_csv('./csv/train.csv', delimiter=';', on_bad_lines='skip')
print(flights.head(5))