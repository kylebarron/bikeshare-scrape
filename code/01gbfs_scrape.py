import requests
import pandas as pd
import pymysql
# https://github.com/PyMySQL/PyMySQL
from sqlalchemy import create_engine

url_system_alerts       = "https://gbfs.thehubway.com/gbfs/en/system_alerts.json"
url_system_regions      = "https://gbfs.thehubway.com/gbfs/en/system_regions.json"
url_system_information  = "https://gbfs.thehubway.com/gbfs/en/system_information.json"
url_station_status      = "https://gbfs.thehubway.com/gbfs/en/station_status.json"
url_station_information = "https://gbfs.thehubway.com/gbfs/en/station_information.json"

password = open("/home/kyle/.config/mysql_kyle_passwd", 'r').read().splitlines()[0]

# For now I'm just going to deal with station_status, since the others don't change as often


data_station_status = requests.get(url_station_status).json()

df_station_status = pd.io.json.json_normalize(data_station_status['data']['stations'])
df_station_status['last_updated'] = data_station_status['last_updated']
df_station_status['last_updated'] = pd.to_datetime(df_station_status['last_updated'], unit = 's')
df_station_status['last_reported'] = pd.to_datetime(df_station_status['last_reported'], unit = 's')

test = create_engine('mysql+pymysql://kyle:' + password + '@localhost/hubway')

df_station_status.to_sql(name='station_status', con = test, if_exists= 'append', index = False)



df_station_status

