import pandas as pd
import os
import time

path = "./datasets/aqi/"
files = os.listdir(path)

aqi = pd.DataFrame(columns=['timestamp', 'city', 'AQI'])

for file in files:
    data = pd.read_csv(path+file)
    data = data[data['hour']==0]

    for index, row in data.iterrows():
        timestamp_str = f"{row['date']} {row['hour']:02d}"
        dt = time.strptime(timestamp_str, '%Y%m%d %H')
        timestamp = int(time.mktime(dt)*1000)

        for city in data.columns[3:]:
                value = row[city]
                if pd.notna(value):
                    if row['type'] == 'AQI':
                         aqi.loc[len(aqi)] =[timestamp, city, value]

    print(file + "已经写入完成！")
    
df = pd.read_csv('./city/province.csv')

aqi = pd.merge(aqi,df, how='left',on='city').dropna()

aqi.to_csv('./data/aqi/aqi.csv', index=False)

print("所有文件已完成写入！")