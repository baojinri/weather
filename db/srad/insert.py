import pandas as pd
import requests
import os

path = "./process_data/srad/"
files = os.listdir(path)

headers = {
    'Content-Type': 'text/plain'
}
data = '''
CREATE TABLE `srad` (
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
    if file != '2018_temp.csv':
       print(file)
       continue
    df = pd.read_csv(path+file)

    for index, row in df.iterrows():
      insert_query = f'''
      INSERT INTO srad (t, province, city, value)
                VALUES ({row['time']}, "{row['province']}", "{row['city']}", {row['srad']})
      '''
      requests.post('http://ceresdb-daily.alibaba.net:5441/sql', headers=headers, data=insert_query)

    print(file+ "数据插入完成")