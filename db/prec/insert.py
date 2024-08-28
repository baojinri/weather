import pandas as pd
import requests
import os

path = "./data/prec/"
files = os.listdir(path)

headers = {
    'Content-Type': 'text/plain'
}
data = '''
CREATE TABLE `prec` (
    `province` string TAG,
    `city` string TAG,
    `value` double NOT NULL,
    `t` timestamp NOT NULL,
    timestamp KEY (t))
ENGINE=Analytic
  with
(enable_ttl="false", update_mode="APPEND")
'''

requests.post('http://localhost:5440/sql', headers= headers, data=data)

for file in files:
    df = pd.read_csv(path+file)

    for index, row in df.iterrows():
      insert_query = f'''
      INSERT INTO prec (t, province, city, value)
                VALUES ({row['time']}, "{row['province']}", "{row['city']}", {row['prec']})
      '''
      requests.post('http://localhost:5440/sql', headers=headers, data=insert_query)

    print(file+ "数据插入完成")