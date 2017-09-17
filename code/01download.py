import requests
import zipfile
import os


## Download Hubway Data
stations_dict = {
    "Hubway_Stations_2011_2016" : "https://s3.amazonaws.com/hubway-data/Hubway_Stations_2011_2016.csv",
    "Hubway_Stations_as_of_July_2017" : "https://s3.amazonaws.com/hubway-data/Hubway_Stations_as_of_July_2017.csv"
}

trip_data_csv = {
    "hubway_Trips_2011" : "https://s3.amazonaws.com/hubway-data/hubway_Trips_2011.csv",
    "hubway_Trips_2012" : "https://s3.amazonaws.com/hubway-data/hubway_Trips_2012.csv",
    "hubway_Trips_2013" : "https://s3.amazonaws.com/hubway-data/hubway_Trips_2013.csv",
    "hubway_Trips_2014_1" : "https://s3.amazonaws.com/hubway-data/hubway_Trips_2014_1.csv",
    "hubway_Trips_2014_2" : "https://s3.amazonaws.com/hubway-data/hubway_Trips_2014_2.csv",
}

trip_data_zip = {
    "201501" : "https://s3.amazonaws.com/hubway-data/201501-hubway-tripdata.zip",
    "201502" : "https://s3.amazonaws.com/hubway-data/201502-hubway-tripdata.zip",
    "201503" : "https://s3.amazonaws.com/hubway-data/201503-hubway-tripdata.zip",
    "201504" : "https://s3.amazonaws.com/hubway-data/201504-hubway-tripdata.zip",
    "201505" : "https://s3.amazonaws.com/hubway-data/201505-hubway-tripdata.zip",
    "201506" : "https://s3.amazonaws.com/hubway-data/201506-hubway-tripdata.zip",
    "201507" : "https://s3.amazonaws.com/hubway-data/201507-hubway-tripdata.zip",
    "201508" : "https://s3.amazonaws.com/hubway-data/201508-hubway-tripdata.zip",
    "201509" : "https://s3.amazonaws.com/hubway-data/201509-hubway-tripdata.zip",
    "201510" : "https://s3.amazonaws.com/hubway-data/201510-hubway-tripdata.zip",
    "201511" : "https://s3.amazonaws.com/hubway-data/201511-hubway-tripdata.zip",
    "201512" : "https://s3.amazonaws.com/hubway-data/201512-hubway-tripdata.zip",
    "201601" : "https://s3.amazonaws.com/hubway-data/201601-hubway-tripdata.zip",
    "201602" : "https://s3.amazonaws.com/hubway-data/201602-hubway-tripdata.zip",
    "201603" : "https://s3.amazonaws.com/hubway-data/201603-hubway-tripdata.zip",
    "201604" : "https://s3.amazonaws.com/hubway-data/201604-hubway-tripdata.zip",
    "201605" : "https://s3.amazonaws.com/hubway-data/201605-hubway-tripdata.zip",
    "201606" : "https://s3.amazonaws.com/hubway-data/201606-hubway-tripdata.zip",
    "201607" : "https://s3.amazonaws.com/hubway-data/201607-hubway-tripdata.zip",
    "201608" : "https://s3.amazonaws.com/hubway-data/201608-hubway-tripdata.zip",
    "201609" : "https://s3.amazonaws.com/hubway-data/201609-hubway-tripdata.zip",
    "201610" : "https://s3.amazonaws.com/hubway-data/201610-hubway-tripdata.zip",
    "201611" : "https://s3.amazonaws.com/hubway-data/201611-hubway-tripdata.zip",
    # "201612" : "https://s3.amazonaws.com/hubway-data/201612-hubway-tripdata.zip",
    "201701" : "https://s3.amazonaws.com/hubway-data/201701-hubway-tripdata.zip",
    "201702" : "https://s3.amazonaws.com/hubway-data/201702-hubway-tripdata.zip",
    "201703" : "https://s3.amazonaws.com/hubway-data/201703-hubway-tripdata.zip",
    "201704" : "https://s3.amazonaws.com/hubway-data/201704-hubway-tripdata.zip",
    "201705" : "https://s3.amazonaws.com/hubway-data/201705-hubway-tripdata.zip",
    "201706" : "https://s3.amazonaws.com/hubway-data/201706-hubway-tripdata.zip",
    "201707" : "https://s3.amazonaws.com/hubway-data/201707-hubway-tripdata.zip",
    "201708" : "https://s3.amazonaws.com/hubway-data/201708-hubway-tripdata.zip"
}

for key, value in trip_data_csv.items():
    file = requests.get(value)
    with open(
        os.path.join("data", "raw", "hubway", "tripdata", key + ".csv"),
        "wb"
    ) as f:
        f.write(file.content)

for key, value in stations_dict.items():
    file = requests.get(value)
    with open(
        os.path.join("data", "raw", "hubway", "stations", key + ".csv"),
        "wb"
    ) as f:
        f.write(file.content)


for key, value in trip_data_zip.items():
    file = requests.get(value)
    with open(
        os.path.join("data", "raw", "hubway", "tripdata", key + ".zip"),
        "wb"
    ) as f:
        f.write(file.content)
    with zipfile.ZipFile(
        os.path.join("data", "raw", "hubway", "tripdata", key + ".zip"),
        "r"
    ) as zip_ref:
        zip_ref.extractall(
            os.path.join("data", "raw", "hubway", "tripdata")
        )
        
# Download Other Boston GIS Data:

## Contour Data:
# Link: https://data.boston.gov/dataset/contours
contours = requests.get("http://bostonopendata-boston.opendata.arcgis.com/datasets/50d4342a5d5941339d4a44839d0fd220_0.zip")
with open(os.path.join("data", "raw", "gis", "contours", "Contours.zip"), "wb") as f:
    f.write(contours.content)
with zipfile.ZipFile(os.path.join("data", "raw", "gis", "contours", "Contours.zip"), "r") as zip_ref:
    zip_ref.extractall(os.path.join("data", "raw", "gis", "contours"))

## Traffic Signals
## https://data.boston.gov/dataset/traffic-signals
signals = requests.get("http://bostonopendata-boston.opendata.arcgis.com/datasets/de08c6fe69c942509089e6db98c716a3_0.zip")
with open(os.path.join("data", "raw", "gis", "traffic_signals", "Traffic_Signals.zip"), "wb") as f:
    f.write(signals.content)
with zipfile.ZipFile(os.path.join("data", "raw", "gis", "traffic_signals", "Traffic_Signals.zip"), "r") as zip_ref:
    zip_ref.extractall(os.path.join("data", "raw", "gis", "traffic_signals"))

## Boston Boundary
## https://data.boston.gov/dataset/city-of-boston-boundary
boundary = requests.get("http://bostonopendata-boston.opendata.arcgis.com/datasets/142500a77e2a4dbeb94a86f7e0b568bc_0.zip")
with open(os.path.join("data", "raw", "gis", "boundary", "boundary.zip"), "wb") as f:
    f.write(boundary.content)
with zipfile.ZipFile(os.path.join("data", "raw", "gis", "boundary", "boundary.zip"), "r") as zip_ref:
    zip_ref.extractall(os.path.join("data", "raw", "gis", "boundary"))

## Bike Network
## https://data.boston.gov/dataset/existing-bike-network
bike_network = requests.get("http://bostonopendata-boston.opendata.arcgis.com/datasets/d02c9d2003af455fbc37f550cc53d3a4_0.zip")
with open(os.path.join("data", "raw", "gis", "bike_network", "bike_network.zip"), "wb") as f:
    f.write(bike_network.content)
with zipfile.ZipFile(os.path.join("data", "raw", "gis", "bike_network", "bike_network.zip"), "r") as zip_ref:
    zip_ref.extractall(os.path.join("data", "raw", "gis", "bike_network"))

## Curbs
## https://data.boston.gov/dataset/curbs
curbs = requests.get("http://bostonopendata-boston.opendata.arcgis.com/datasets/35945c894a604bd49fca7a9f0e3f124a_6.zip")
with open(os.path.join("data", "raw", "gis", "curbs", "curbs.zip"), "wb") as f:
    f.write(curbs.content)
with zipfile.ZipFile(os.path.join("data", "raw", "gis", "curbs", "curbs.zip"), "r") as zip_ref:
    zip_ref.extractall(os.path.join("data", "raw", "gis", "curbs"))

## Sidewalks
## https://data.boston.gov/dataset/sidewalk-inventory
sidewalks = requests.get("http://bostonopendata-boston.opendata.arcgis.com/datasets/6aa3bdc3ff5443a98d506812825c250a_0.zip")
with open(os.path.join("data", "raw", "gis", "sidewalks", "sidewalks.zip"), "wb") as f:
    f.write(sidewalks.content)
with zipfile.ZipFile(os.path.join("data", "raw", "gis", "sidewalks", "sidewalks.zip"), "r") as zip_ref:
    zip_ref.extractall(os.path.join("data", "raw", "gis", "sidewalks"))

## Sidewalk Centerline
## https://data.boston.gov/dataset/sidewalk-centerline
sidewalk_centerline = requests.get("http://bostonopendata-boston.opendata.arcgis.com/datasets/bbbf06c5327e476dacdcb3dc0c9b3ddb_5.zip")
with open(os.path.join("data", "raw", "gis", "sidewalk_centerline", "sidewalk_centerline.zip"), "wb") as f:
    f.write(sidewalk_centerline.content)
with zipfile.ZipFile(os.path.join("data", "raw", "gis", "sidewalk_centerline", "sidewalk_centerline.zip"), "r") as zip_ref:
    zip_ref.extractall(os.path.join("data", "raw", "gis", "sidewalk_centerline"))

## Streets
## https://data.boston.gov/dataset/boston-segments
streets = requests.get("http://bostonopendata-boston.opendata.arcgis.com/datasets/cfd1740c2e4b49389f47a9ce2dd236cc_8.zip")
with open(os.path.join("data", "raw", "gis", "streets", "streets.zip"), "wb") as f:
    f.write(streets.content)
with zipfile.ZipFile(os.path.join("data", "raw", "gis", "streets", "streets.zip"), "r") as zip_ref:
    zip_ref.extractall(os.path.join("data", "raw", "gis", "streets"))


# MBTA Data:
# T: http://www.mass.gov/anf/research-and-tech/it-serv-and-support/application-serv/office-of-geographic-information-massgis/datalayers/mbta.html
# Bus: http://www.mass.gov/anf/research-and-tech/it-serv-and-support/application-serv/office-of-geographic-information-massgis/datalayers/mbtabus.html
subway = requests.get("http://wsgw.mass.gov/data/gispub/shape/state/mbta_rapid_transit.zip")
with open(os.path.join("data", "raw", "gis", "mbta", "subway.zip"), "wb") as f:
    f.write(subway.content)
with zipfile.ZipFile(os.path.join("data", "raw", "gis", "mbta", "subway.zip"), "r") as zip_ref:
    zip_ref.extractall(os.path.join("data", "raw", "gis", "mbta"))

bus = requests.get("http://wsgw.mass.gov/data/gispub/shape/state/mbtabus.zip")
with open(os.path.join("data", "raw", "gis", "mbta", "bus.zip"), "wb") as f:
    f.write(bus.content)
with zipfile.ZipFile(os.path.join("data", "raw", "gis", "mbta", "bus.zip"), "r") as zip_ref:
    zip_ref.extractall(os.path.join("data", "raw", "gis", "mbta"))

# Cambridge Contours:

