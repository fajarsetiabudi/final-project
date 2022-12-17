#!Pyhton3.8

from pymongo import MongoClient
import pandas as pd
from sqlalchemy import create_engine
import numpy as np

# connecting to MongoDB 
client = MongoClient("mongodb+srv://belajar:belajar@cluster0.isoxsv8.mongodb.net/?retryWrites=true&w=majority")
sample_training = client["sample_training"]
collection= sample_training["companies"]

# connection to postgres
url = 'postgresql+psycopg2://postgres:admin@localhost:5432/mongo_etl'
engine = create_engine(url)

array_list = [
    '_id','offices','image', 'products', 'relationships', 'competitions', 'providerships', 
    'funding_rounds', 'investments','acquisition','acquisitions','milestones','video_embeds',
    'screenshots','external_links','partners', 'ipo'
    ]

fields = collection.aggregate([
    {"$addFields": {
    "office": {"$first": "$offices"}
        }},
    {"$unset" : array_list}
    ], allowDiskUse=True)
df_companies = pd.DataFrame.from_dict(list(fields))

office_flatte = {
    'description': '',
    'address1': '',
    'address2': '',
    'zip_code': '',
    'city': '',
    'state_code': '',
    'country_code': '',
    'latitude': None,
    'longitude': None
    }
df_companies['office'] = np.where(df_companies['office'].notna(), df_companies['office'], office_flatte)

office = pd.DataFrame(df_companies['office'].tolist()) 
companies_df = pd.concat([df_companies, office], axis=1 )
companies_df.drop(['office'], axis=1, inplace=True)
companies_df.head()

# export data to postgres
try:
    companies_df.to_sql('companies', index=False, con=engine, if_exists='replace')
    
    print("Data succes export to postgres")
except:
    print("data failed")