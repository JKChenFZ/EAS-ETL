import sys
from load_history.loader import Loader

# TODO? check command line argument

if __name__ == '__main__':
    test_mode = True if len(sys.argv) == 3 else False
    load = Loader(sys.argv[1], mode=test_mode)
    load.run()


