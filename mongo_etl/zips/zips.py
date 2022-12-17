#!Pyhton3.8

from pymongo import MongoClient
import pandas as pd
from sqlalchemy import create_engine
import numpy as np

# connecting to MongoDB 
client = MongoClient("mongodb+srv://belajar:password@cluster0.isoxsv8.mongodb.net/")
sample_training = client["sample_training"]
collection_zips= sample_training["zips"]

# connection to postgres
url = 'postgresql+psycopg2://postgres:admin@localhost:5432/mongo_etl'
engine = create_engine(url)

cursor_zips = collection_zips.find({}, {'_id': 0})
df_zips = pd.DataFrame.from_dict(list(cursor_zips))

zips_field = pd.DataFrame(df_zips['loc'].tolist())
df_zips = pd.concat([df_zips, zips_field], axis=1)
df_zips.drop(['loc'], axis=1, inplace=True)
df_zips.rename(columns={'x': 'latitude', 'y': 'longitude'}, inplace=True)
df_zips.head()

# export data to postgres
try:
    df_zips.to_sql('zips', index=False, con=engine, if_exists='replace')
    
    print("Data succes export to postgres")
except:
    print("data failed")