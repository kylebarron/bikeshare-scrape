import os
import pandas as pd
import psycopg2
import re
import requests
import time
from pandas.io.json   import json_normalize
from pandas.api.types import is_string_dtype
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
    station_status = json_normalize(request['data']['stations'])
    station_status['last_updated'] = request['last_updated']
    print("Downloaded station_status %s seconds" % round(time.time() - start_time, 2))

    # Extract the number from station_id if there's text in there
    try:
        station_status['station_id'] = station_status['station_id'].astype(int)
    except ValueError:
        match_list = station_status['station_id'].str.findall(r'[0-9]+')
        station_status['station_id'] = match_list.str[0].astype(int)
        print("Finished extracting number from station_id %s seconds" % round(time.time() - start_time, 2))

    # Now create id as numeric(string(station_id) + string(last_reported))
    station_status['id'] = (
        station_status['last_reported'].apply(str) +
        station_status['station_id'].apply(str)
    ).apply(int)
    
    # Put last_reported and last_updated in datetime format
    for var in ['last_reported', 'last_updated']:
        # Replace NaN with 1 for the timestamps
        station_status[var] = station_status[var].fillna(value = 1)
        try:
            station_status[var] = pd.to_datetime(station_status[var], unit = 's')
        except:
            print(system_id + ' felt like using milliseconds')
            station_status[var] = pd.to_datetime(station_status[var], unit = 'ms')
            station_status[var] = station_status[var] / 1000
            station_status[var].fillna(0, inplace = True)
            station_status[var] = station_status[var].astype(int)
        
    station_status.is_installed = station_status.is_installed.astype(bool)
    station_status.is_renting   = station_status.is_renting.astype(bool)
    station_status.is_returning = station_status.is_returning.astype(bool)
    try:
        station_status.eightd_has_available_keys = station_status.eightd_has_available_keys.astype(bool)
    except AttributeError:
        pass
    
    toadd = station_status[[
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
        if var in station_status.columns:
            toadd[var] = station_status[var]

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

    insert_stmt = insert(table).on_conflict_do_nothing(
        index_elements = ['id']
    )
    # To check the SQL generated is correct: print(str(insert_stmt))

    conn = engine.connect()
    print("Finished making engine connection %s seconds" % round(time.time() - start_time, 2))

    conn.execute(insert_stmt, toadd.to_dict('records'))
    print("Finished inserting records into Postgresql %s seconds" % round(time.time() - start_time, 2))

    conn.close()
    print("--- Total time: %s seconds ---" % round(time.time() - start_time, 2))


main()

# url = 'https://tor.publicbikesystem.net/ube/gbfs/v1/en/station_status'
# system_id = 'bike_share_toronto'
#
