import os
import pandas as pd
import psycopg2
import re
import requests
from geoalchemy2 import Geometry
from sqlalchemy         import create_engine
from sqlalchemy.schema  import CreateSchema
from sqlalchemy_utils   import database_exists, create_database, drop_database
from sqlalchemy         import MetaData, Column, Table, ForeignKey
from sqlalchemy.dialects.postgresql import \
    ARRAY, BIGINT, BIT, BOOLEAN, BYTEA, CHAR, CIDR, DATE, \
    DOUBLE_PRECISION, ENUM, FLOAT, HSTORE, INET, INTEGER, \
    INTERVAL, JSON, JSONB, MACADDR, NUMERIC, OID, REAL, SMALLINT, TEXT, \
    TIME, TIMESTAMP, UUID, VARCHAR, INT4RANGE, INT8RANGE, NUMRANGE, \
    DATERANGE, TSRANGE, TSTZRANGE, TSVECTOR

def main():
    password = open("/home/kyle/.config/postgres_passwd", 'r').read().splitlines()[0]
    systems = pd.read_csv(os.path.join("..", "data", "gbfs_systems.csv"))

    # Get port used:
    # https://stackoverflow.com/questions/16904997/connection-refused-pgerror-postgresql-and-rails
    config = open('/etc/postgresql/10/main/postgresql.conf').read()
    port = re.search(r'port = (\d{4})', config)[1]

    # Create one database for all bikeshare tables
    engine = create_engine('postgresql+psycopg2://kyle:' + password + '@localhost:' + port + '/bikeshare')
    if not database_exists(engine.url):
        engine_kyle = create_engine('postgresql+psycopg2://kyle:' + password + '@localhost:' + port + '/kyle')
        conn = engine_kyle.connect()
        create_database(engine.url)
        conn.close()
    
    try:
        engine.execute('CREATE EXTENSION postgis;')
    except:
        pass

    # Create a data frame of urls for every bikeshare provider
    url_df = pd.DataFrame()
    for i in range(len(systems)):
        # First go to the system's gbfs site
        try:
            gbfs = requests.get(systems['Auto-Discovery URL'][i]).json()
            gbfs_urls = pd.io.json.json_normalize(gbfs['data']['en']['feeds'])
            gbfs_urls['System ID'] = systems['System ID'][i]
            url_df = url_df.append(gbfs_urls)
        except:
            ConnectionError

    # Remove all the rows with 'name' == 'gbfs' to prevent infinite recursion
    url_df = url_df[url_df['name'] != 'gbfs']
    url_df.to_csv(os.path.join('..', 'data', 'url_list.csv'))
    
    # Somewhere around here, I also did manually (in psql):
    # GRANT ALL privileges on DATABASE bikeshare TO kyle;
    # GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO kyle;

    # Create schema for each System ID
    unique_systems = list(set(url_df['System ID'].tolist()))
    for i in range(len(unique_systems)):
        try:
            engine.execute(CreateSchema(unique_systems[i]))
        except:
            pass

    # Create tables
    for i in range(len(url_df)):
        table_name = url_df['name'].tolist()[i]
        schema_name = url_df['System ID'].tolist()[i]
        if not engine.dialect.has_table(engine, table_name, schema = schema_name):
            create_table(table_name, schema_name, password)

def create_table(table_name, schema_name, password):
    engine = create_engine('postgresql+psycopg2://kyle:' + password + '@localhost:' + port + '/bikeshare')
    metadata = MetaData(bind = engine, schema = schema_name)
    # if table_name == 'system_information':
    #     table = Table(
    #         'system_information',
    #         metadata,
    #         Column('last_updated', Timestamp, primary_key = True),
    #         Column('system_id', VARCHAR(255)),
    #         Column('language', VARCHAR(255)),
    #         Column('timezone', VARCHAR(255))
    #     )
    #     metadata.create_all()
    #
    if table_name == 'station_information':
        table = Table(
            'station_information',
            metadata,
            Column('id', BIGINT, primary_key = True),
            Column('station_id', SMALLINT),
            Column('name', VARCHAR(255)),
            Column('lat_lon', Geometry(geometry_type='POINT', srid=4326)),
            Column('address', VARCHAR(255)),
            Column('cross_street', VARCHAR(255)),
            Column('post_code', VARCHAR(255)),
            Column('region_id', VARCHAR(255)),
            Column('capacity', SMALLINT),
            Column('last_updated', TIMESTAMP),
            Column('eightd_has_key_dispenser', BOOLEAN))
        metadata.create_all()
    
    if table_name == 'station_status':
        table = Table(
            'station_status',
            metadata,
            Column('id', BIGINT, primary_key = True),
            Column('station_id', SMALLINT),
            Column('num_bikes_available', SMALLINT),
            Column('num_bikes_disabled', SMALLINT),
            Column('num_docks_available', SMALLINT),
            Column('num_docks_disabled', SMALLINT),
            Column('is_installed', BOOLEAN),
            Column('is_renting', BOOLEAN),
            Column('is_returning', BOOLEAN),
            Column('last_reported', TIMESTAMP),
            Column('last_updated', TIMESTAMP),
            Column('eightd_has_available_keys', BOOLEAN))
        metadata.create_all()

    if table_name == 'free_bike_status':
        table = Table(
            'free_bike_status',
            metadata,
            Column('id', BIGINT, primary_key = True),
            Column('bike_id', SMALLINT),
            Column('lat_lon', Geometry(geometry_type='POINT', srid=4326)),
            Column('is_reserved', BOOLEAN),
            Column('is_disabled', BOOLEAN),
            Column('last_updated', TIMESTAMP))
        metadata.create_all()

    # if table_name == 'system_hours':
    #     table = Table(
    #     'system_hours',
    #         metadata,
    #         Column('id'),
    #         Column('user_types'),
    #         Column('days'),
    #         Column('start_time'),
    #         Column('end_time'))
    #     metadata.create_all()

    if table_name == 'system_calendar':
        table = Table(
            'system_calendar',
            metadata,
            Column('id', INTEGER, primary_key = True),
            Column('start_month', SMALLINT),
            Column('start_day', SMALLINT),
            Column('start_year', SMALLINT),
            Column('end_month', SMALLINT),
            Column('end_day', SMALLINT),
            Column('end_year', SMALLINT))
        metadata.create_all()

    if table_name == 'system_regions':
        table = Table(
            'system_regions',
            metadata,
            Column('id', INTEGER, primary_key = True),
            Column('region_id', SMALLINT),
            Column('name', VARCHAR(255)))
        metadata.create_all()

    if table_name == 'system_pricing_plans':
        table = Table(
            'system_pricing_plans',
            metadata,
            Column('plan_id', VARCHAR(255), primary_key = True),
            Column('name', VARCHAR(255)),
            Column('currency', CHAR(3)),
            Column('price', NUMERIC(15, 2)),
            Column('is_taxable', BOOLEAN),
            Column('description', VARCHAR(255)))
        metadata.create_all()

    if table_name == 'system_alerts':
        table = Table(
            'system_alerts',
            metadata,
            Column('alert_id', VARCHAR(255), primary_key = True),
            Column('type', VARCHAR(255)),
            Column('time_start', TIMESTAMP),
            Column('time_end', TIMESTAMP),
            Column('station_ids', VARCHAR(65535)),
            Column('region_ids', VARCHAR(65535)),
            Column('summary', VARCHAR(255)),
            Column('description', VARCHAR(255)),
            Column('last_updated', TIMESTAMP))
        metadata.create_all()

main()

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

