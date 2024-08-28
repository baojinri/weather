import pandas as pd
import os

# path = "./datasets/城市_20210101-20211231/"
# files = os.listdir(path)

# city_df = pd.DataFrame(columns=['city'])
# df = pd.read_csv('./city/city.csv')

# for file in files:
#     data = pd.read_csv(path+file)

#     for index, row in data.iterrows():
#         for city in data.columns[3:]:
#             city_df.loc[len(city_df)] =[city]
#         break
#     break
            
# province = pd.merge(city_df, df, how='left', on=['city'] )


# province.to_csv('./city/province2.csv', index=False)


df = pd.read_csv('./city/province.csv')

df =df[['province', 'city']]
df['city'] = df['city'].apply(lambda value: value.rstrip(value[-1]))
df = df.dropna()
df.to_csv('./city/province2.csv', index=False)
