import requests
import datetime
from cognite.client.data_classes import Asset
from cognite.client.data_classes import TimeSeries
from cognite.client import CogniteClient

def retrieve_or_create_asset(client, asset_data):
    asset = client.assets.retrieve(external_id=asset_data.external_id)
    if not asset:
        asset = client.assets.create(asset_data)
    return asset

def create_countries(client, locations):
    for location in locations:
        country = location['country']
        province = location['province']
        country_external_id = country
        country_asset = retrieve_or_create_asset(client, Asset(name=country, external_id=country_external_id, parent_external_id="covid19"))
        if province != "":
            province_external_id = country_external_id+"_"+province
            province_asset = retrieve_or_create_asset(client, Asset(name=province, external_id=province_external_id, parent_external_id=country_external_id))
            
def create_time_series(client, data):
    types = ['confirmed', 'deaths', 'recovered']
    subtree = client.assets.retrieve_subtree(external_id='covid19')
    time_series = []
    for asset in subtree:
        for t in types:
            external_id = asset.external_id+"_"+t
            name = asset.external_id+" "+t
            time_series.append(TimeSeries(name=name, legacy_name=external_id, external_id=external_id, asset_id=asset.id))
    client.time_series.create(time_series)

def handle(client):
    data = requests.get(url="http://coronavirus-tracker-api.herokuapp.com/all").json()
    covid19 = retrieve_or_create_asset(client, Asset(name="Covid19", external_id="covid19"))
    create_countries(client, data['confirmed']['locations'])
    create_countries(client, data['deaths']['locations'])
    create_countries(client, data['recovered']['locations'])
    create_time_series(client, data)
    print("Created or updated AH+TS")
    types = ['confirmed', 'deaths', 'recovered']
    country_sum = {}
    for t in types:
        for location in data[t]['locations']:
            country = location['country']
            country_external_id = country
            if not country in country_sum:
                country_sum[country] = {
                    'confirmed': {},
                    'deaths': {},
                    'recovered': {}
                }
            province = location['province']
            points = location['history']
            
            datapoints = []
            for date, value in points.items():
                dt = datetime.datetime.strptime(date, "%m/%d/%y")
                datapoints.append((dt, value))
                if not date in country_sum[country][t]:
                    country_sum[country][t][date] = 0
                country_sum[country][t][date] += value

            if province:
                external_id = f"{country_external_id}_{province}_{t}"
            else:
                external_id = f"{country_external_id}_{t}"
            client.datapoints.insert(datapoints, external_id=external_id)
    print("Created data points for provinces")
    for country, types in country_sum.items():
        for t, values in types.items():
            datapoints = []
            for date, value in values.items():
                dt = datetime.datetime.strptime(date, "%m/%d/%y")
                datapoints.append((dt, value))
            external_id = f"{country}_{t}"
            client.datapoints.insert(datapoints, external_id=external_id)
    print("Created data points for countries")

if __name__ == "__main__":
    client = CogniteClient()
    handle(client)