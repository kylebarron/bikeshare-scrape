import requests
import pandas as pd
from pandas.io.json import json_normalize
from pandas.api.types import is_string_dtype
from sqlalchemy import MetaData, Column, Table, ForeignKey
from sqlalchemy import create_engine
from sqlalchemy.dialects.mysql import \
        BIGINT, BOOLEAN, INTEGER, \
        LONGBLOB, LONGTEXT, MEDIUMBLOB, MEDIUMINT, MEDIUMTEXT, NCHAR, \
        NUMERIC, NVARCHAR, REAL, SET, SMALLINT, TIME, TIMESTAMP, \
        TINYINT, YEAR
import re
import time
import os

def main():
    start_time = time.time()
    password = open("/home/kyle/.config/mysql_kyle_passwd", 'r').read().splitlines()[0]
    print("\nFinished reading password %s seconds" % (time.time() - start_time))

    url_df  = pd.read_csv(os.path.join("home", "kyle", "Documents", "research", "personal", "bikeshare-scrape", "data", "url_list.csv"))
    print("Imported url_list %s seconds" % (time.time() - start_time))

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
    print('Attempting this for type=' + type + ' url=' + url + ' system_id=' + system_id)
    if type == 'station_status':
        # Retrieve Data:
        request = requests.get(url).json()
        station_status = json_normalize(request['data']['stations'])
        print("Downloaded station_status %s seconds" % (time.time() - start_time))
        
        station_status['last_updated'] = request['last_updated']
        
        # Put last_reported in datetime format
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
            print('system_id=' + system_id + "\nFinished extracting number from station_id %s seconds" % (time.time() - start_time))
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
        print('system_id=' + system_id + "\nFinished making id variable %s seconds" % (time.time() - start_time))
        
        toadd = station_status[[
            'id',
            'station_id_num',
            'num_bikes_available',
            'num_docks_available',
            'is_installed',
            'is_renting',
            'is_returning',
            'last_reported_stamp',
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
        print('system_id=' + system_id + "\nFinished making toadd df %s seconds" % (time.time() - start_time))
        
        
        engine = create_engine('mysql://kyle:' + password + '@localhost/' + system_id)
        print('system_id=' + system_id + "\nFinished creating engine %s seconds" % (time.time() - start_time))
        
        
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
        inserter = table.insert().prefix_with('IGNORE')
        print('system_id=' + system_id + "\nFinished declaring insert statement %s seconds" % (time.time() - start_time))
        
        conn = engine.connect()
        print('system_id=' + system_id + "\nFinished making engine connection %s seconds" % (time.time() - start_time))
        
        conn.execute(inserter, toadd.to_dict('records'))
        print('system_id=' + system_id + "\nFinished inserting records into mysql %s seconds" % (time.time() - start_time))
        
        print("--- Total time: %s seconds ---" % (time.time() - start_time))
        
        # http://docs.sqlalchemy.org/en/latest/dialects/mysql.html#insert-on-duplicate-key-update-upsert
        # insert_stmt = 

main() 

# url = 'https://tor.publicbikesystem.net/ube/gbfs/v1/en/station_status'
# system_id = 'bike_share_toronto'
# 
