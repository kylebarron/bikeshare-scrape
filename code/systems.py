"""Get list of GBFS feeds in US as JSON
"""
import json

import pandas as pd


def main():
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
    print(json.dumps(d, separators=(',', ':')))


if __name__ == '__main__':
    main()
