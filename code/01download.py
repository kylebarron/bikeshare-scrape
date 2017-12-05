# This file downloads the file with URLs of each bikeshare system.
import requests
import pandas as pd
import os

gbfs_systems = requests.get("https://raw.githubusercontent.com/NABSA/gbfs/master/systems.csv")
with open(os.path.join("..", "data", "gbfs_systems.csv"), "wb") as f:
    f.write(gbfs_systems.content)

systems = pd.read_csv(os.path.join("..", "data", "gbfs_systems.csv"))
systems['System ID'] = systems['System ID'].str.lower()

systems.to_csv(os.path.join("..", "data", "gbfs_systems.csv"))
