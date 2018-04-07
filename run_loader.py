import sys
from load_history.loader import Loader

# TODO? check command line argument

if __name__ == '__main__':
    load = Loader(sys.argv[1])
    load.run()


