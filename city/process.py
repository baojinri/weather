import json
import pandas as pd

with open('./city/data.json', 'r') as file:
    data = json.load(file)

data = [value for value in data  if value['area']==""]
data = pd.DataFrame(data)
data = data[["province", "city", "lat", "lng"]]
data['lat'] = pd.to_numeric(data['lat']).astype(float).round(1)
data['lon'] = pd.to_numeric(data['lng']).astype(float).round(1)
data.drop('lng', axis=1, inplace=True)

data.to_csv('./city/city.csv', index=False)