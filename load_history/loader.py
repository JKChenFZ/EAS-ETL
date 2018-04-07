import boto3
import requests
import sys
from decimal import Decimal
from load_history.util import get_country


class Loader(object):
    def __init__(self, file_path, limit=50, mode=False):
        self.file_path = file_path
        self.table = boto3.resource('dynamodb').Table('eas-earthquake')
        self.current_data = []
        self.limit = limit
        self.batch_counter = 0
        self.upload_counter = 0
        self.date_ranges = []
        self.request_url = 'https://earthquake.usgs.gov/fdsnws/event/1/query'
        self.current_range = None
        self.test_mode = mode

    def batch_write(self):
        print('[Batch writer started] Counter: ' + str(self.upload_counter))
        batch_size = len(self.current_data)

        try:
            with self.table.batch_writer() as batch:
                for e in self.current_data:
                    batch.put_item(Item=e)
        except Exception as e:
            print('[Error] Something went wrong with batch writer, check error')
            print(e)
            print('[Error] Shutting down')

            self.write_remainig_into_file()
            sys.exit()

        self.current_data.clear()
        self.batch_counter = 0
        self.upload_counter += batch_size

    def read_ranges_from_file(self):
        with open(self.file_path, 'r') as lines:
            for line in lines:
                split = line.rstrip().split(':')
                self.date_ranges.append((split[0], split[1]))

    def request_data(self):
        for e in self.date_ranges:
            self.current_range = e

            start = e[0]
            end = e[1]
            pay_load = {
                'format': 'geojson',
                'starttime': start,
                'endtime': end
            }

            r = requests.get(self.request_url, params=pay_load)
            response = r.json()
            print('[Info] Fetch from ' + str(e))

            for data in response['features']:

                if self.test_mode and self.upload_counter >= 50:
                    print('exiting due to test mode')
                    sys.exit()

                if data['id'] is not None and data['geometry'] is not None and len(
                        data['geometry']['coordinates']) == 3 and data['properties'] is not None:
                    self.current_data.append({
                        'id': data['id'],
                        'incident_type': 'Feature' if data['type'] is None else str(data['type']),
                        'geotype': str(data['geometry']['type']),
                        'longitude': Decimal(str(data['geometry']['coordinates'][0])),
                        'latitude': Decimal(str(data['geometry']['coordinates'][1])),
                        'depth': Decimal(str(data['geometry']['coordinates'][2])),
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
                    self.batch_counter += 1

                    if self.batch_counter >= self.limit:
                        self.batch_write()

    def run(self):
        self.read_ranges_from_file()
        self.request_data()
        print('[Load finished] Upload counter: ' + str(self.upload_counter))

    def write_remainig_into_file(self):
        if self.current_range is None:
            return

        should_write = False
        with open('failed_ranges.txt', 'w') as out:
            for c_range in self.date_ranges:
                if c_range == self.current_data:
                    should_write = True

                if should_write:
                    out.write(str(c_range[0]) + ':' + str(c_range[1]) + '\n')

        print('[Write] Finished cleaning up')

if __name__ == '__main__':
    mode = True if sys.argv[2] == 't' else False
    load = Loader(sys.argv[1], mode=mode)
    load.run()
