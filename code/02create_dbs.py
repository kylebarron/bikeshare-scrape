import requests
import pandas as pd
import pymysql
import os
from sqlalchemy import create_engine
from sqlalchemy import MetaData, Column, Table, ForeignKey
from sqlalchemy_utils import database_exists, create_database, drop_database
from sqlalchemy.dialects.mysql import \
        BIGINT, BINARY, BIT, BLOB, BOOLEAN, CHAR, DATE, \
        DATETIME, DECIMAL, DOUBLE, ENUM, FLOAT, INTEGER, \
        LONGBLOB, LONGTEXT, MEDIUMBLOB, MEDIUMINT, MEDIUMTEXT, NCHAR, \
        NUMERIC, NVARCHAR, REAL, SET, SMALLINT, TEXT, TIME, TIMESTAMP, \
        TINYBLOB, TINYINT, TINYTEXT, VARBINARY, VARCHAR, YEAR

def main():
    password = open("/home/kyle/.config/mysql_kyle_passwd", 'r').read().splitlines()[0]

    systems = pd.read_csv(os.path.join("home", "kyle", "Documents", "research", "personal", "bikeshare-scrape", "data", "gbfs_systems.csv"))

    # Create a database for each bikeshare provider within MySQL
    for i in systems['System ID']:
        engine = create_engine('mysql+pymysql://kyle:' + password + '@localhost/' + i)
        if database_exists(engine.url):
            drop_database(engine.url)
            create_database(engine.url)
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

    # Remove all the rows with 'name' == 'gbfs' to prevent infinite recursion
    url_df = url_df[url_df['name'] != 'gbfs']
    url_df = url_df[url_df['System ID'] != 'curtin_university']
    url_df.to_csv(os.path.join('..', 'data', 'url_list.csv'))
    
    # Create tables
    for i in range(1, len(url_df)):
        # Make connection
        engine = create_engine('mysql+pymysql://kyle:' + password + '@localhost/' + url_df['System ID'].tolist()[i])
        if not engine.dialect.has_table(engine, url_df['name'].tolist()[i]):
            create_table(url_df['name'].tolist()[i], password, url_df['System ID'].tolist()[i])

def aside():    
    # See how often data gets reloaded
    ttl_df = pd.DataFrame()
    for i in range(1, len(systems)):
        ttl = requests.get(systems['Auto-Discovery URL'][i]).json()
        ttl = pd.io.json.json_normalize(ttl)
        ttl['System ID'] = systems['System ID'][i]
        ttl_df = ttl_df.append(ttl[['System ID', 'ttl']])
        
    ttl_df[ttl_df['ttl'] != 60]
    # All except Abu Dhabi have 60, Abu Dhabi has 10
    # Therefore collecting data every minute is reasonable


def create_table(type, password, system_id):
    
    # if type == 'system_information':
    #     engine = create_engine('mysql+pymysql://kyle:' + password + '@localhost/' + system_id)
    #     metadata = MetaData(bind = engine)
    #     table = Table(
    #         'system_information',
    #         metadata,
    #         Column('last_updated', Timestamp, primary_key = True),
    #         Column('system_id', VARCHAR(28)),
    #         Column('language', VARCHAR(3)),
    #         Column('timezone', VARCHAR(35))
    #     )
    #     metadata.create_all()
    # 
    # if type == 'station_information':
    #     engine = create_engine('mysql+pymysql://kyle:' + password + '@localhost/' + system_id)
    #     metadata = MetaData(bind = engine)
    #     table = Table(
    #         'station_information',
    #         metadata,
    #         Column('station_information_id', ),
    #         Column('station_id', ),
    #         Column('name', ),
    #         Column('lat', DECIMAL(9,6)),
    #         Column('lon', DECIMAL(9,6)),
    #         Column('region_id', ),
    #         Column('capacity', SmallInteger),
    #         Column('eightd_has_key_dispenser', boolean),
    #     )
    #     metadata.create_all()
    #     # - rental_methods	Optional	Array of enumerables containing the payment methods accepted at this station. 
    
    if type == 'station_status':
        engine = create_engine('mysql+pymysql://kyle:' + password + '@localhost/' + system_id)
        metadata = MetaData(bind = engine)
        table = Table(
            'station_status',
            metadata,
            Column('id', BIGINT(unsigned = True), primary_key = True),
            Column('station_id', SMALLINT(unsigned = True)),
            Column('num_bikes_available', TINYINT(unsigned = True)),
            Column('num_bikes_disabled', TINYINT(unsigned = True)),
            Column('num_docks_available', TINYINT(unsigned = True)),
            Column('num_docks_disabled', TINYINT(unsigned = True)),
            Column('is_installed', BOOLEAN),
            Column('is_renting', BOOLEAN),
            Column('is_returning', BOOLEAN),
            Column('last_reported', TIMESTAMP),
            Column('last_updated', TIMESTAMP),
            Column('eightd_has_available_keys', BOOLEAN)
        )
        metadata.create_all()
    
    if type == 'free_bike_status':
        engine = create_engine('mysql+pymysql://kyle:' + password + '@localhost/' + system_id)
        metadata = MetaData(bind = engine)
        table = Table(
            'free_bike_status',
            metadata,
            Column('id', INTEGER(unsigned = True), primary_key = True),
            Column('bike_id', SMALLINT(unsigned = True)),
            Column('lat', DECIMAL(9,6)),
            Column('lon', DECIMAL(9,6)),
            Column('is_reserved', BOOLEAN),
            Column('is_disabled', BOOLEAN),
            Column('last_updated', TIMESTAMP)
        )
        metadata.create_all()

    # if type == 'system_hours':
    #     engine = create_engine('mysql+pymysql://kyle:' + password + '@localhost/' + system_id)
    #     metadata = MetaData(bind = engine)
    #     table = Table(
    #     'system_hours',
    #     metadata,
    #     Column('id'),
    #     Column('user_types'),
    #     Column('days'),
    #     Column('start_time'),
    #     Column('end_time'),
    #     
    #     )
    #     metadata.create_all()
    
    if type == 'system_calendar':
        engine = create_engine('mysql+pymysql://kyle:' + password + '@localhost/' + system_id)
        metadata = MetaData(bind = engine)
        table = Table(
            'system_calendar',
            metadata,
            Column('id', INTEGER(unsigned = True), primary_key = True),
            Column('start_month', TINYINT(unsigned = True)),
            Column('start_day', TINYINT(unsigned = True)),
            Column('start_year', YEAR),
            Column('end_month', TINYINT(unsigned = True)),
            Column('end_day', TINYINT(unsigned = True)),
            Column('end_year', YEAR)
        )
        metadata.create_all()

    if type == 'system_regions': 
        engine = create_engine('mysql+pymysql://kyle:' + password + '@localhost/' + system_id)
        metadata = MetaData(bind = engine)
        table = Table(
            'system_regions',
            metadata,
            Column('id', INTEGER(unsigned = True), primary_key = True),
            Column('region_id', SMALLINT(unsigned = True)),
            Column('name', VARCHAR(50))
        )
        metadata.create_all()

    # if type == 'system_pricing_plans':
    #     engine = create_engine('mysql+pymysql://kyle:' + password + '@localhost/' + system_id)
    #     metadata = MetaData(bind = engine)
    #     table = Table(
    #         'system_pricing_plans',
    #         metadata,
    #         Column('id', INTEGER(unsigned = True), primary_key = True),
    #         Column('plan_id'),
    #         Column('url'),
    #         Column('name'),
    #         Column('currency'),
    #         Column('price'),
    #         Column('is_taxable', BOOLEAN),
    #         Column('description'),
    #     )
    #     metadata.create_all()

    # if type == 'system_alerts':
    #     engine = create_engine('mysql+pymysql://kyle:' + password + '@localhost/' + system_id)
    #     metadata = MetaData(bind = engine)
    #     table = Table(
    #             'system_alerts',
    #             metadata,
    #             
    #         )
    #     metadata.create_all()
    


main()