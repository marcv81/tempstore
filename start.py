import tempstore.engine as ts_e
import tempstore.webapp as ts_wa

import argparse

BASE_URL = 'http://localhost:8000'

# Instantiates the engine.
engine = ts_e.Engine('datastore', 'database', 30*24*60*60)

# Instantiates the WSGI app.
app = ts_wa.App(engine, BASE_URL)

# Uses the engine from the command line.
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--init',
        help='bootstrap or reset the contents',
        action='store_true')
    parser.add_argument(
        '--cleanup',
        help='clean up the obsolete versions and unreferrenced blobs',
        action='store_true')
    args = parser.parse_args()
    if args.init:
        engine.create()
    if args.cleanup:
        engine.cleanup()
