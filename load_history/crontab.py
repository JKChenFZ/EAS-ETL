import boto3
import requests
import sys
from decimal import Decimal
from numbers import Number
from load_history.util import get_country
import time


class EASCrontabUtil(object):
    def __init__(self, limit=300, mode=False):
        self.table = boto3.resource('dynamodb').Table('eas-earthquake-prod')
        self.live_table = boto3.resource('dynamodb').Table('eas-earthquake-prod-live')
        self.current_data = []
        self.limit = limit
        self.upload_counter = 0
        self.request_url = 'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson'
        self.test_mode = mode
        self.fetch_counter = 0
        self.dynamo_paginator = boto3.client('dynamodb').get_paginator('scan')

    def batch_write(self):
        print('[Batch writer started] Counter: ' + str(self.upload_counter))
        batch_size = len(self.current_data)

        try:
            with self.table.batch_writer() as batch:
                for e in self.current_data:
                    batch.put_item(Item=e)

            with self.live_table.batch_writer() as batch:
                for e in self.current_data:
                    batch.put_item(Item=e)
        except Exception as e:
            print('[Error] Something went wrong with batch writer, check error')
            print(e)
            print('[Error] Shutting down')

            sys.exit()

        self.current_data.clear()
        self.upload_counter += batch_size
        print('[Batch writer Ended]')

    def request_data(self):

        r = requests.get(self.request_url)
        response = r.json()
        self.fetch_counter += len(response['features'])
        print('[Info] ' + str(time.time()) + ' Fetch with ' + str(len(response['features'])))

        for data in response['features']:

            if data['id'] is not None and data['geometry'] is not None and len(
                    data['geometry']['coordinates']) == 3 and data['properties'] is not None:
                self.current_data.append({
                    'id': data['id'],
                    'incident_type': 'Feature' if data['type'] is None else str(data['type']),
                    'geotype': str(data['geometry']['type']),
                    'longitude': Decimal(str(data['geometry']['coordinates'][0])),
                    'latitude': Decimal(str(data['geometry']['coordinates'][1])),
                    'depth': Decimal(str(data['geometry']['coordinates'][2])) if isinstance(data['geometry']['coordinates'][2], Number) else Decimal('0'),
                    'mag': 0 if data['properties']['mag'] is None else Decimal(str(data['properties']['mag'])),
                    'place': 'N/A' if data['properties']['place'] is None else str(data['properties']['place']),
                    'time': -1 if data['properties']['time'] is None else Decimal(str(data['properties']['time'])),
                    'url': 'N/A' if data['properties']['url'] is None else str(data['properties']['url']),
                    'tsunami': 0 if data['properties']['tsunami'] is None else Decimal(
                        str(data['properties']['tsunami'])),
                    'title': 'N/A' if data['properties']['title'] is None else str(data['properties']['title']),
                    'country': get_country(data['geometry']['coordinates'][1], data['geometry']['coordinates'][0]),
                    'tz': 0 if data['properties']['tz'] is None else Decimal(str(data['properties']['tz'])),
                    'status': 'N/A' if data['properties']['status'] is None else str(data['properties']['status']),
                    'magnitude_type': 'N/A' if data['properties']['magType'] is None else str(
                        data['properties']['magType']),
                    'nst': 0 if data['properties']['nst'] is None else Decimal(str(data['properties']['nst']))
                })

        self.batch_write()

    def run_loader(self):
        print('[Crontab hourly loading] started')
        self.request_data()
        print('[Crontab Load finished] Upload counter: ' + str(self.upload_counter))
        print('[Compare] ' + str(self.upload_counter == self.fetch_counter))

    def run_clean(self):
        res_iterator = self.dynamo_paginator.paginate(TableName='eas-earthquake-prod-live')
        id_list = []
        for page in res_iterator:
            for e in page['Items']:
                id_list.append(e['id']['S'])

        print('[Crontab clean] number to clean ' + str(len(id_list)))

        for e_id in id_list:
            self.live_table.delete_item(
                Key={
                    'id': e_id
                }
            )

        print('[Crontab clean] finished')

if __name__ == '__main__':
    pass
