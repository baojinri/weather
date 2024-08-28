import pandas as pd
import requests
import os

path = "./process_data/pm/"
files = os.listdir(path)

headers = {
    'Content-Type': 'text/plain'
}

# data = '''
# CREATE TABLE `pm2` (
#     `province` string TAG,
#     `city` string TAG,
#     `value` double NOT NULL,
#     `t` timestamp NOT NULL,
#     timestamp KEY (t))
# ENGINE=Analytic
#   with
# (enable_ttl="false", update_mode="APPEND")
# '''
# requests.post('http://ceresdb-daily.alibaba.net:5441/sql', headers= headers, data=data)

# data = '''
# CREATE TABLE `pm10` (
#     `province` string TAG,
#     `city` string TAG,
#     `value` double NOT NULL,
#     `t` timestamp NOT NULL,
#     timestamp KEY (t))
# ENGINE=Analytic
#   with
# (enable_ttl="false", update_mode="APPEND")
# '''
# requests.post('http://ceresdb-daily.alibaba.net:5441/sql', headers= headers, data=data)

# data = '''
# CREATE TABLE `so2` (
#     `province` string TAG,
#     `city` string TAG,
#     `value` double NOT NULL,
#     `t` timestamp NOT NULL,
#     timestamp KEY (t))
# ENGINE=Analytic
#   with
# (enable_ttl="false", update_mode="APPEND")
# '''
# requests.post('http://ceresdb-daily.alibaba.net:5441/sql', headers= headers, data=data)

# data = '''
# CREATE TABLE `no2` (
#     `province` string TAG,
#     `city` string TAG,
#     `value` double NOT NULL,
#     `t` timestamp NOT NULL,
#     timestamp KEY (t))
# ENGINE=Analytic
#   with
# (enable_ttl="false", update_mode="APPEND")
# '''
# requests.post('http://ceresdb-daily.alibaba.net:5441/sql', headers= headers, data=data)

# data = '''
# CREATE TABLE `o3` (
#     `province` string TAG,
#     `city` string TAG,
#     `value` double NOT NULL,
#     `t` timestamp NOT NULL,
#     timestamp KEY (t))
# ENGINE=Analytic
#   with
# (enable_ttl="false", update_mode="APPEND")
# '''
# requests.post('http://ceresdb-daily.alibaba.net:5441/sql', headers= headers, data=data)

# data = '''
# CREATE TABLE `co` (
#     `province` string TAG,
#     `city` string TAG,
#     `value` double NOT NULL,
#     `t` timestamp NOT NULL,
#     timestamp KEY (t))
# ENGINE=Analytic
#   with
# (enable_ttl="false", update_mode="APPEND")
# '''
# requests.post('http://ceresdb-daily.alibaba.net:5441/sql', headers= headers, data=data)

data = '''
CREATE TABLE `aqi_2018` (
    `province` string TAG,
    `city` string TAG,
    `value` double NOT NULL,
    `t` timestamp NOT NULL,
    timestamp KEY (t))
ENGINE=Analytic
  with
(enable_ttl="false", update_mode="APPEND")
'''
requests.post('http://ceresdb-daily.alibaba.net:5441/sql', headers= headers, data=data)

for file in files:
    df = pd.read_csv(path+file)
    if file == '2018_aqi.csv':
       for index, row in df.iterrows():
        insert_query = f'''
        INSERT INTO aqi_2018 (t, province, city, value)
                  VALUES ({row['timestamp']}, "{row['province']}", "{row['city']}", {row['AQI']})
        '''
        requests.post('http://ceresdb-daily.alibaba.net:5441/sql', headers=headers, data=insert_query)
        continue

    if file == '2021_no2_.csv':
      #  for index, row in df.iterrows():
      #   insert_query = f'''
      #   INSERT INTO no2 (t, province, city, value)
      #             VALUES ({row['timestamp']}, "{row['province']}", "{row['city']}", {row['NO2']})
      #   '''
      #   requests.post('http://ceresdb-daily.alibaba.net:5441/sql', headers=headers, data=insert_query)
        continue
    
    if file == '2021_co_.csv':
      #  for index, row in df.iterrows():
      #   insert_query = f'''
      #   INSERT INTO co (t, province, city, value)
      #             VALUES ({row['timestamp']}, "{row['province']}", "{row['city']}", {row['CO']})
      #   '''
      #   requests.post('http://ceresdb-daily.alibaba.net:5441/sql', headers=headers, data=insert_query)
      continue

    if file == '2021_pm2_.csv':
      #  for index, row in df.iterrows():
      #   insert_query = f'''
      #   INSERT INTO pm2 (t, province, city, value)
      #             VALUES ({row['timestamp']}, "{row['province']}", "{row['city']}", {row['PM2.5']})
      #   '''
      #   requests.post('http://ceresdb-daily.alibaba.net:5441/sql', headers=headers, data=insert_query)
      continue
    
    # if file == '2021_so2_.csv':
    #    for index, row in df.iterrows():
    #     insert_query = f'''
    #     INSERT INTO so2 (t, province, city, value)
    #               VALUES ({row['timestamp']}, "{row['province']}", "{row['city']}", {row['SO2']})
    #     '''
    #     requests.post('http://ceresdb-daily.alibaba.net:5441/sql', headers=headers, data=insert_query)
    
    # if file == '2021_o3_.csv':
    #    for index, row in df.iterrows():
    #     insert_query = f'''
    #     INSERT INTO o3 (t, province, city, value)
    #               VALUES ({row['timestamp']}, "{row['province']}", "{row['city']}", {row['O3']})
    #     '''
    #     requests.post('http://ceresdb-daily.alibaba.net:5441/sql', headers=headers, data=insert_query)
    
    # if file == '2021_pm10_.csv':
    #    for index, row in df.iterrows():
    #     insert_query = f'''
    #     INSERT INTO pm10 (t, province, city, value)
    #               VALUES ({row['timestamp']}, "{row['province']}", "{row['city']}", {row['PM10']})
    #     '''
    #     requests.post('http://ceresdb-daily.alibaba.net:5441/sql', headers=headers, data=insert_query)

    print(file+ "数据插入完成")