import geocoder


def get_country(latitude, longitude):
    try:
        result = geocoder.google([latitude, longitude], method='reverse').country
        if result is None:
            return 'NONE'
        else:
            return result
    except ValueError as e:
        # print(e)
        return 'NONE'


if __name__ == '__main__':
    print(get_country('-122.846735', '37.580407'))
    print(get_country(37.580407, -122.846735))
    # 37.580407, -122.846735