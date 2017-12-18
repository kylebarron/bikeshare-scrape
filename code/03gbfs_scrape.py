import os
import pandas as pd
import psycopg2
import re
import requests
import time
from pandas.io.json   import json_normalize
from pandas.api.types import is_string_dtype
from shapely.geometry import Point
from sqlalchemy       import MetaData, Column, Table, ForeignKey
from sqlalchemy       import create_engine
from sqlalchemy.dialects.postgresql import \
    ARRAY, BIGINT, BIT, BOOLEAN, BYTEA, CHAR, CIDR, DATE, \
    DOUBLE_PRECISION, ENUM, FLOAT, HSTORE, INET, INTEGER, INTERVAL, \
    insert, JSON, JSONB, MACADDR, NUMERIC, OID, REAL, SMALLINT, TEXT, \
    TIME, TIMESTAMP, UUID, VARCHAR, INT4RANGE, INT8RANGE, NUMRANGE, \
    DATERANGE, TSRANGE, TSTZRANGE, TSVECTOR

def main():
    start_time = time.time()
    password = open("/home/kyle/.config/postgres_passwd", 'r').read().splitlines()[0]
    print("Finished reading password %s seconds" % round(time.time() - start_time, 2))

    url_df  = pd.read_csv(os.path.join("..", "data", "url_list.csv"))
    print("Imported url_list %s seconds" % round(time.time() - start_time, 2))
    
    # Get port used:
    # https://stackoverflow.com/questions/16904997/connection-refused-pgerror-postgresql-and-rails
    config = open('/etc/postgresql/10/main/postgresql.conf').read()
    port = re.search(r'port = (\d{4})', config)[1]
    
    for i in range(len(url_df)):
        if url_df['name'][i] == 'station_status':
            try:
                get_station_status(
                    type = url_df['name'][i],
                    url = url_df['url'][i],
                    system_id = url_df['System ID'][i],
                    password = password,
                    port = port,
                    start_time = start_time)
            except Exception as e:
                print(e)
        elif url_df['name'][i] == 'free_bike_status':
            try:
                get_free_bike_status(
                    type = url_df['name'][i],
                    url = url_df['url'][i],
                    system_id = url_df['System ID'][i],
                    password = password,
                    port = port,
                    start_time = start_time)
            except Exception as e:
                print(e)

def get_station_status(
    type,
    url,
    system_id,
    password,
    port,
    start_time):
    
    print('Attempting data retrieval for:\ntype = ' + type + '\nsystem_id = ' + system_id + '\nurl=' + url)

    # Retrieve Data:
    request = requests.get(url).json()
    df = json_normalize(request['data']['stations'])
    df['last_updated'] = request['last_updated']
    print("Downloaded station_status %s seconds" % round(time.time() - start_time, 2))

    # Extract the number from station_id if there's text in there
    try:
        df['station_id'] = df['station_id'].astype(int)
    except ValueError:
        match_list = df['station_id'].str.findall(r'[0-9]+')
        df['station_id'] = match_list.str[0].astype(int)
        print("Finished extracting number from station_id %s seconds" % round(time.time() - start_time, 2))

    # Now create id as numeric(string(station_id) + string(last_reported))
    df['id'] = (
        df['last_reported'].apply(str) +
        df['station_id'].apply(str)
    ).apply(int)
    
    # Put last_reported and last_updated in datetime format
    for var in ['last_reported', 'last_updated']:
        # Replace NaN with 1 for the timestamps
        df[var] = df[var].fillna(value = 1)
        try:
            df[var] = pd.to_datetime(df[var], unit = 's')
        except:
            print(system_id + ' felt like using milliseconds')
            df[var] = pd.to_datetime(df[var], unit = 'ms')
            df[var] = df[var] / 1000
            df[var].fillna(0, inplace = True)
            df[var] = df[var].astype(int)
        
    df.is_installed = df.is_installed.astype(bool)
    df.is_renting   = df.is_renting.astype(bool)
    df.is_returning = df.is_returning.astype(bool)
    try:
        df.eightd_has_available_keys = df.eightd_has_available_keys.astype(bool)
    except AttributeError:
        pass
    
    toadd = df[[
        'id',
        'station_id',
        'num_bikes_available',
        'num_docks_available',
        'is_installed',
        'is_renting',
        'is_returning',
        'last_reported',
        'last_updated'
    ]]

    for var in ['num_bikes_disabled', 'num_docks_disabled', 'eightd_has_available_keys']:
        if var in df.columns:
            toadd[var] = df[var]

    print("Finished making toadd df %s seconds" % round(time.time() - start_time, 2))

    engine = create_engine('postgresql+psycopg2://kyle:' + password + '@localhost:' + port + '/bikeshare')
    print("Finished creating engine %s seconds" % round(time.time() - start_time, 2))

    metadata = MetaData(bind = engine, schema = system_id)
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
        Column('eightd_has_available_keys', BOOLEAN)
    )

    insert_stmt = insert(table).on_conflict_do_nothing(index_elements = ['id'])
    # To check the SQL generated is correct: print(str(insert_stmt))

    conn = engine.connect()
    print("Finished making engine connection %s seconds" % round(time.time() - start_time, 2))

    conn.execute(insert_stmt, toadd.to_dict('records'))
    print("Finished inserting records into Postgresql %s seconds" % round(time.time() - start_time, 2))

    conn.close()
    print("--- Total time: %s seconds ---" % round(time.time() - start_time, 2))

def get_free_bike_status(
    type,
    url,
    system_id,
    password,
    port,
    start_time):
    
    print('Attempting data retrieval for:\ntype = ' + type + '\nsystem_id = ' + system_id + '\nurl=' + url)

    # Retrieve Data:
    request = requests.get(url).json()
    df = json_normalize(request['data']['bikes'])
    df['last_updated'] = request['last_updated']
    print("Downloaded free_bike_status %s seconds" % round(time.time() - start_time, 2))
        
    # Extract the number from bike_id if there's text in there
    try:
        df['bike_id'] = df['bike_id'].astype(int)
    except ValueError:
        match_list = df['bike_id'].str.findall(r'[0-9]+')
        df['bike_id'] = match_list.str[0].astype(int)
        print("Finished extracting number from bike_id %s seconds" % round(time.time() - start_time, 2))

    # Now create id as numeric(string(station_id) + string(last_updated))
    df['id'] = (
        df['last_updated'].apply(str) +
        df['bike_id'].apply(str)
    ).apply(int)

    # Put last_updated in datetime format
    # Replace NaN with 1 for the timestamps
    df['last_updated'] = df['last_updated'].fillna(value = 1)
    try:
        df['last_updated'] = pd.to_datetime(df['last_updated'], unit = 's')
    except:
        print(system_id + ' felt like using milliseconds')
        df['last_updated'] = pd.to_datetime(df['last_updated'], unit = 'ms')
        df['last_updated'] = df['last_updated'] / 1000
        df['last_updated'].fillna(0, inplace = True)
        df['last_updated'] = df['last_updated'].astype(int)

    df['lat_lon'] = 'SRID=4326;' + df[['lon', 'lat']].apply(Point, axis = 1).astype(str)
    
    df = df.drop(columns = ['name', 'lat', 'lon'])
    
    df.is_reserved = df.is_reserved.astype(bool)
    df.is_disabled = df.is_disabled.astype(bool)
    
    engine = create_engine('postgresql+psycopg2://kyle:' + password + '@localhost:' + port + '/bikeshare')
    print("Finished creating engine %s seconds" % round(time.time() - start_time, 2))

    metadata = MetaData(bind = engine, schema = system_id)
    table = Table(
        'free_bike_status',
        metadata,
        Column('id', BIGINT, primary_key = True),
        Column('bike_id', SMALLINT),
        Column('lat_lon', Geometry(geometry_type='POINT', srid=4326)),
        Column('is_reserved', BOOLEAN),
        Column('is_disabled', BOOLEAN),
        Column('last_updated', TIMESTAMP))
    
    insert_stmt = insert(table).on_conflict_do_nothing(index_elements = ['id'])
    # To check the SQL generated is correct: print(str(insert_stmt))
    
    conn = engine.connect()
    print("Finished making engine connection %s seconds" % round(time.time() - start_time, 2))

    conn.execute(insert_stmt, df.to_dict('records'))
    print("Finished inserting records into Postgresql %s seconds" % round(time.time() - start_time, 2))

    conn.close()
    print("--- Total time: %s seconds ---" % round(time.time() - start_time, 2))


main()

# url = 'https://tor.publicbikesystem.net/ube/gbfs/v1/en/station_status'
# system_id = 'bike_share_toronto'
#
