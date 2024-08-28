import xarray as xr
import pandas as pd
import os
import math

path = "./datasets/lrad/"
files = os.listdir(path)

for file in files:
    file_year = file[40:44]
    ds = xr.open_dataset(path + file)

    ds['time'] = pd.to_datetime(ds['time'].values).astype('int64') / 10**6
    ds = ds.to_dataframe().reset_index()
    ds['lat'] = ds['lat'].apply(lambda v: math.trunc(v*10)/10).astype(float)
    ds['lon'] = ds['lon'].apply(lambda v: math.trunc(v*10)/10).astype(float)

    cities_df = pd.read_csv('./city/city.csv')

    merged_df = pd.merge(cities_df, ds, how='left', on=['lon', 'lat'])

    result_df = merged_df[['province', 'city', 'time', 'lrad']]
    result_df['time'] = result_df['time'].astype(int)

    result_df.to_csv("./process_data/lrad/{}_temp.csv".format(file_year), index=False)
                
    print(file + "已经写入完成！")

print("所有文件已完成写入！")