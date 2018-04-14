from load_history.crontab import EASCrontabUtil

if __name__ == '__main__':
    loader = EASCrontabUtil()
    loader.run_loader()
