import requests
import sys


class Counter(object):
    def __init__(self, file_path):
        self.request_url = 'https://earthquake.usgs.gov/fdsnws/event/1/query'
        self.file_path = file_path
        self.date_ranges = []
        self.counter = 0

    def read_ranges_from_file(self):
        with open(self.file_path, 'r') as lines:
            for line in lines:
                split = line.rstrip().split(':')
                self.date_ranges.append((split[0], split[1]))

    def process_dates(self):
        for e in self.date_ranges:
            start = e[0]
            end = e[1]
            pay_load = {
                'format': 'geojson',
                'starttime': start,
                'endtime': end
            }

            r = requests.get(self.request_url, params=pay_load)
            response = r.json()
            print('[Info] Fetch from ' + str(e) + ' with ' + str(len(response['features'])))
            self.counter += len(response['features'])

    def run(self):
        self.read_ranges_from_file()
        self.process_dates()
        print('[Final count] for ' + str(self.file_path) + ' : ' + str(self.counter))

if __name__ == '__main__':
    count = Counter(sys.argv[1])
    count.run()


