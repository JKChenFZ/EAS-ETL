from load_history.crontab import EASCrontabUtil
import datetime

if __name__ == '__main__':
    print('--------------Start-----------------------------')
    print(datetime.datetime.utcnow())
    loader = EASCrontabUtil()
    loader.run_loader()
    print('--------------Done------------------------------')

