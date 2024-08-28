import pandas as pd
import os
import time

path = "./datasets/2018_air/"
files = os.listdir(path)

aqi = pd.DataFrame(columns=['timestamp', 'city', 'AQI'])

count =0
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
    count = count + 1
    print(count)
    print(file)
    
df = pd.read_csv('./city/province2.csv')

aqi = pd.merge(aqi,df, how='left',on='city').dropna()


# 保存结果到 CSV 文件
aqi.to_csv('./process_data/pm/2018_aqi.csv', index=False)
