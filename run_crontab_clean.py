import datetime
from load_history.crontab import EASCrontabUtil

if __name__ == '__main__':
    print('--------------Start-----------------------------')
    print(datetime.datetime.utcnow())
    util = EASCrontabUtil()
    util.run_clean()
    print('--------------Done------------------------------')
