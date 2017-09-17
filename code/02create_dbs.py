import requests
import pandas as pd
import pymysql
from sqlalchemy import create_engine
from sqlalchemy import MetaData, Column, Table, ForeignKey
from sqlalchemy import Integer, String, DateTime
from sqlalchemy_utils import database_exists, create_database
import os

password = open("/home/kyle/.config/mysql_kyle_passwd", 'r').read().splitlines()[0]
test = create_engine('mysql+pymysql://kyle:' + password + '@localhost')

systems = pd.read_csv(os.path.join("..", "data", "gbfs_systems.csv"))

# Create a database for each bikeshare provider within MySQL
for i in systems['System ID']:
    engine = create_engine('mysql+pymysql://kyle:' + password + '@localhost/' + i)
    if not database_exists(engine.url):
        create_database(engine.url)

# Create a data frame of urls for every bikeshare provider
url_df = pd.DataFrame()
for i in range(1, len(systems)):
    # First go to the system's gbfs site
    gbfs = requests.get(systems['Auto-Discovery URL'][i]).json()
    gbfs_urls = pd.io.json.json_normalize(gbfs['data']['en']['feeds'])
    gbfs_urls['System ID'] = systems['System ID'][i]
    url_df = url_df.append(gbfs_urls)
    
url_df
    
    
for i in 
    engine = create_engine('mysql+pymysql://kyle:' + password + '@localhost/' + systems['System ID'][i])
    metadata=MetaData(bind=engine)
    main_table=Table('sample',metadata,
                Column('LIN',String(10),primary_key=True),
                Column('material_type',String(20),nullable=False),
                Column('source',String(20),nullable=False),
                Column('material_description',String(100)),
                Column('quantity',Integer),
                Column('location',String(2)),
                Column('received_by',String(20)),
                Column('received_date',DateTime,nullable=False),
                )
    metadata.create_all()
    print(i)


test2 = requests.get("https://gbfs.bcycle.com/bcycle_arborbike/gbfs.json")
test2 = test2.json()
test2 = pd.io.json.json_normalize(test2['data']['en']['feeds'])
test2


colnames
test = gbfs.json() 
pd.io.json.json_normalize(test['data']['en']['feeds'])
list(systems.columns.values)


engine = create_engine('mysql+pymysql://kyle:' + password + '@localhost/mydb')
database_exists(engine.url)
if not database_exists(engine.url):
    create_database(engine.url)

print(database_exists(engine.url))