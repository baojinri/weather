import pandas as pd
import os
import time

path = "./datasets/air/"
files = os.listdir(path)

pm2 = pd.DataFrame(columns=['timestamp', 'city', 'PM2.5'])
pm10 = pd.DataFrame(columns=['timestamp', 'city', 'PM10'])
so2 = pd.DataFrame(columns=['timestamp', 'city', 'SO2'])
no2 = pd.DataFrame(columns=['timestamp', 'city', 'NO2'])
o3 = pd.DataFrame(columns=['timestamp', 'city', 'O3'])
co = pd.DataFrame(columns=['timestamp', 'city', 'CO'])
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
                    if row['type'] == 'PM2.5':
                         pm2.loc[len(pm2)] =[timestamp, city, value]
                    if row['type'] == 'PM10':
                         pm10.loc[len(pm10)] =[timestamp, city, value]
                    if row['type'] == 'SO2':
                         so2.loc[len(so2)] =[timestamp, city, value]
                    if row['type'] == 'NO2':
                         no2.loc[len(no2)] =[timestamp, city, value]
                    if row['type'] == 'O3':
                         o3.loc[len(o3)] =[timestamp, city, value]
                    if row['type'] == 'CO':
                         co.loc[len(co)] =[timestamp, city, value]

    count = count +1
    print(count)
    print(file)
    
df = pd.read_csv('./city/province2.csv')

aqi = pd.merge(aqi,df, how='left',on='city').dropna()
pm2 = pd.merge(pm2,df, how='left',on='city').dropna()
pm10 = pd.merge(pm10,df, how='left',on='city').dropna()
so2 = pd.merge(so2,df, how='left',on='city').dropna()
no2 = pd.merge(no2,df, how='left',on='city').dropna()
o3 = pd.merge(o3,df, how='left',on='city').dropna()
co = pd.merge(co,df, how='left',on='city').dropna()


# 保存结果到 CSV 文件
aqi.to_csv('./process_data/pm/2021_aqi.csv', index=False)
pm2.to_csv('./process_data/pm/2021_pm2.csv', index=False)
pm10.to_csv('./process_data/pm/2021_pm10.csv', index=False)
so2.to_csv('./process_data/pm/2021_so2.csv', index=False)
no2.to_csv('./process_data/pm/2021_no2.csv', index=False)
o3.to_csv('./process_data/pm/2021_o3.csv', index=False)
co.to_csv('./process_data/pm/2021_co.csv', index=False)
