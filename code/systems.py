"""Get list of GBFS feeds in US as JSON
"""
import json

import pandas as pd
import requests


def main():
    d = get_system_urls()
    for record in d:
        url = record.pop('url')
        record['feeds'] = get_feed_urls(url)

    print(json.dumps(d, separators=(',', ':')))


def get_system_urls():
    url = 'https://raw.githubusercontent.com/NABSA/gbfs/master/systems.csv'
    df = pd.read_csv(url)

    # Keep systems in US
    df = df[df['Country Code'] == 'US']

    # Keep Name, Location, System ID, and Auto-Discovery URL
    # and rename to simpler names
    cols = {
        'Name': 'name',
        'Location': 'location',
        'System ID': 'id',
        'Auto-Discovery URL': 'url'}
    df = df[cols.keys()]
    df = df.rename(columns=cols)

    d = df.to_dict(orient='records')
    return d


def get_feed_urls(url):
    r = requests.get(url)
    d = r.json()

    if d.get('data') and d['data'].get('en') and d['data']['en'].get('feeds'):
        feeds = d['data']['en']['feeds']

        # Look specifically for the feeds named _station_information_ and
        # _station_status_ and _free_bike_status_
        names = ['station_information', 'station_status', 'free_bike_status']
        feeds = [x for x in feeds if x['name'] in names]
        return feeds

    return []


if __name__ == '__main__':
    main()
