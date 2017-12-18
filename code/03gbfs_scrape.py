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
    print("\nFinished reading password %s seconds" % round(time.time() - start_time, 2))

    url_df  = pd.read_csv(os.path.join("..", "data", "url_list.csv"))
    print("Imported url_list %s seconds" % round(time.time() - start_time, 2))
    
    for i in range(len(url_df)):
        if url_df['name'][i] == 'station_status':
            try:
                get_data(
                    type = url_df['name'][i],
                    url = url_df['url'][i],
                    password = password,
                    system_id = url_df['System ID'][i],
                    start_time = start_time
                )
            except Exception as e: print(e)

def get_data(type, url, password, system_id, start_time):
    print('Attempting data retrieval for type=' + type + ', url=' + url + ', system_id=' + system_id)

    if type == 'station_status':
        # Retrieve Data:
        request = requests.get(url).json()
        station_status = json_normalize(request['data']['stations'])
        print("Downloaded station_status %s seconds" % round(time.time() - start_time, 2))

        station_status['last_updated'] = request['last_updated']

        # Put last_reported in datetime format
        # Replace NaN with 1 for the timestamps
        station_status['last_reported'] = station_status['last_reported'].fillna(value = 1)
        try:
            station_status['last_reported_stamp'] = pd.to_datetime(station_status['last_reported'], unit = 's')
        except:
            print(system_id + ' felt like using milliseconds')
            station_status['last_reported_stamp'] = pd.to_datetime(station_status['last_reported'], unit = 'ms')
            station_status['last_reported'] = station_status['last_reported'] / 1000
            station_status['last_reported'].fillna(0, inplace = True)
            station_status['last_reported'] = station_status['last_reported'].astype(int)

        # Put last_updated in datetime format
        try:
            station_status['last_updated'] = pd.to_datetime(station_status['last_updated'], unit = 's')
        except:
            print(system_id + ' felt like using milliseconds')
            station_status['last_updated'] = pd.to_datetime(station_status['last_updated'], unit = 'ms')
            station_status['last_updated'] = station_status['last_updated'] / 1000
            station_status['last_updated'].fillna(0, inplace = True)
            station_status['last_updated'] = station_status['last_updated'].astype(int)

        # Extract the number from station_id if there's text in there
        if is_string_dtype(station_status['station_id']):
            if re.search(r'[^0-9]', station_status['station_id'].tolist()[0]):
                match_list = station_status['station_id'].str.findall(r'[0-9]+')
                num_match_list = match_list.str[0].astype(int)
                station_status['station_id_num'] = num_match_list
            else:
                station_status['station_id_num'] = station_status['station_id'].astype(int)
            print('system_id=' + system_id + "\nFinished extracting number from station_id %s seconds" % round(time.time() - start_time, 2))
        else:
            station_status = station_status.rename(
                columns = {
                    'station_id': 'station_id_num'
                }
            )

        # Now create id as numeric(string(station_id) + string(last_reported))
        station_status['id'] = (
            station_status['last_reported'].apply(str) +
            station_status['station_id_num'].apply(str)
        ).apply(int)
        print('system_id=' + system_id + "\nFinished making id variable %s seconds" % round(time.time() - start_time, 2))

        toadd = station_status[[
            'id',
            'station_id_num',
            'num_bikes_available',
            'num_docks_available',
            'is_installed',
            'is_renting',
            'is_returning',
            'last_reported_stamp'
        ]]

        if 'num_bikes_disabled' in station_status.columns:
            toadd['num_bikes_disabled'] = station_status['num_bikes_disabled']
        if 'num_docks_disabled' in station_status.columns:
            toadd['num_docks_disabled'] = station_status['num_docks_disabled']
        if 'eightd_has_available_keys' in station_status.columns:
            toadd['eightd_has_available_keys'] = station_status['eightd_has_available_keys']

        toadd = toadd.rename(
            columns = {
                'station_id_num': 'station_id',
                'last_reported_stamp': 'last_reported'
            }
        )
        print('system_id=' + system_id + "\nFinished making toadd df %s seconds" % round(time.time() - start_time, 2))


        engine = create_engine('postgresql+psycopg2://kyle:' + password + '@localhost:5433/bikeshare')
        print('system_id=' + system_id + "\nFinished creating engine %s seconds" % round(time.time() - start_time, 2))


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
        # To check the SQL generated is correct:
        # print(str(insert_stmt))

        conn = engine.connect()
        print('system_id=' + system_id + "\nFinished making engine connection %s seconds" % round(time.time() - start_time, 2))

        conn.execute(insert_stmt, toadd.to_dict('records'))
        print('system_id=' + system_id + "\nFinished inserting records into mysql %s seconds" % round(time.time() - start_time, 2))

        conn.close()
        print("--- Total time: %s seconds ---" % round(time.time() - start_time, 2))


main()

# url = 'https://tor.publicbikesystem.net/ube/gbfs/v1/en/station_status'
# system_id = 'bike_share_toronto'
#
