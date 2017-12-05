import os
import pandas as pd
import psycopg2
import re
import requests
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

    # Create a data frame of urls for every bikeshare provider
    url_df = pd.DataFrame()
    for i in range(1, len(systems)):
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
    
    # Somewhere around here, I also did manually:
    # GRANT ALL privileges on DATABASE bikeshare TO kyle;
    # GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO kyle;

    # Create schema for each System ID
    unique_systems = list(set(url_df['System ID'].tolist()))
    for i in range(1, len(unique_systems)):
        try:
            engine.execute(CreateSchema(unique_systems[i]))
        except:
            pass
    engine.execute(CreateSchema('bcycle_clarksville'))
    # Create tables
    for i in range(1, len(url_df)):
        if not engine.dialect.has_table(engine, url_df['name'].tolist()[i], schema = url_df['System ID'].tolist()[i]):
            create_table(url_df['name'].tolist()[i], url_df['System ID'].tolist()[i], password)


def create_table(type, system_id, password):
    engine = create_engine('postgresql+psycopg2://kyle:' + password + '@localhost:' + port + '/bikeshare')
    metadata = MetaData(bind = engine, schema = url_df['System ID'].tolist()[i])
    # if type == 'system_information':
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
    #     table = Table(
    #         'station_information',
    #         metadata,
    #         Column('station_information_id', ),
    #         Column('station_id', ),
    #         Column('name', ),
    #         Column('lat', NUMERIC(9,6)),
    #         Column('lon', NUMERIC(9,6)),
    #         Column('region_id', ),
    #         Column('capacity', SmallInteger),
    #         Column('eightd_has_key_dispenser', boolean),
    #     )
    #     metadata.create_all()
    #     # - rental_methods	Optional	Array of enumerables containing the payment methods accepted at this station.

    if type == 'station_status':
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
        metadata.create_all(engine)

    if type == 'free_bike_status':
        table = Table(
            'free_bike_status',
            metadata,
            Column('id', BIGINT, primary_key = True),
            Column('bike_id', SMALLINT),
            Column('lat', NUMERIC(9,6)),
            Column('lon', NUMERIC(9,6)),
            Column('is_reserved', BOOLEAN),
            Column('is_disabled', BOOLEAN),
            Column('last_updated', TIMESTAMP)
        )
        metadata.create_all()

    # if type == 'system_hours':
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
        table = Table(
            'system_calendar',
            metadata,
            Column('id', INTEGER, primary_key = True),
            Column('start_month', SMALLINT),
            Column('start_day', SMALLINT),
            Column('start_year', SMALLINT),
            Column('end_month', SMALLINT),
            Column('end_day', SMALLINT),
            Column('end_year', SMALLINT)
        )
        metadata.create_all()

    if type == 'system_regions':
        table = Table(
            'system_regions',
            metadata,
            Column('id', INTEGER, primary_key = True),
            Column('region_id', SMALLINT),
            Column('name', VARCHAR(50))
        )
        metadata.create_all()

    # if type == 'system_pricing_plans':
    #     table = Table(
    #         'system_pricing_plans',
    #         metadata,
    #         Column('id', INTEGER, primary_key = True),
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
    #     table = Table(
    #             'system_alerts',
    #             metadata,
    #
    #         )
    #     metadata.create_all()



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

