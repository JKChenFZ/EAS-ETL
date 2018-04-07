import reverse_geocoder as rg


# def get_country(latitude, longitude):
#     try:
#         result = geocoder.google([latitude, longitude], method='reverse').country
#         if result is None:
#             return 'NONE'
#         else:
#             return result
#     except ValueError as e:
#         # print(e)
#         return 'NONE'

def get_country(lat, long):
    try:
        result = list(rg.search([(lat, long)])[0].values())[-1]
        if result is None or result == '':
            return 'NONE'
        else:
            return result
    except:
        return 'NONE'

if __name__ == '__main__':
    # print(get_country('-122.846735', '37.580407'))
    print(get_country('-122.846735', '37.580407'))
    # print(get_country(37.580407, -122.846735))
    print(get_country(37.580407, -122.846735))
    print(get_country(32.1321667, -115.8406667))
    # 37.580407, -122.846735
