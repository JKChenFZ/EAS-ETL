from load_history.crontab import EASCrontabUtil

if __name__ == '__main__':
    util = EASCrontabUtil()
    util.run_clean()
