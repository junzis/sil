"""
To use the script:
    $ python merge_pv.py --db DB_NAME --date YYYY_MM_DD

Script will look for the position and velocity collections (DATE_p and DATE_v),
then merge the position with the velocities into a new collection (name: DATE)

So the ideas here is:

A. Get all ICAOs in position collection, the loop throw each
    1. get all positions and velocities of this ICAO
        a. get the velocities within +/- 5s for each position's timestamp
        b. chose closest vh for the position, None if no close vh found
    2. save the constructed flight data in a new collection
"""


import argparse
import numpy as np
from pymongo import MongoClient

# Constants
HOST = "localhost"   # MongoDB host
PORT = 27017         # MongoDB port
MIN_DATA_SIZA = 100  # minimal number of data in a flight
CHUNK_SIZE = 20      # number of icaos to be processed in chunks
TEST_FLAG = True     # weather this is a test run

# get script arguments
parser = argparse.ArgumentParser()
parser.add_argument('--db', dest="db", required=True)
parser.add_argument('--date', dest='date', required=True,
                    help="Date for position and velocity data")
args = parser.parse_args()

db = args.db
date = args.date

mclient = MongoClient('localhost', 27017)

mcollp = mclient[db][date + '_p']
mcollv = mclient[db][date + '_v']
mcollmerge = mclient[db][date]

# clear output database
mcollmerge.drop()

# fetch all icaos
q = mcollp.aggregate([
    {
        '$group': {
            '_id': '$icao',
            'count': {'$sum': 1}
        }
    }
])

icaos = [r['_id'] for r in q['result']]
total_counts = len(icaos)

count = 0
for icao in icaos:

    count += 1
    print "Merging P/V: %d of %d" % (count, total_counts)

    ps = list(mcollp.find({'icao': icao}))
    vs = list(mcollv.find({'icao': icao}))

    if len(ps) < 100 or len(vs) < 100:
        continue

    vsnp = np.array([[v['spd'], v['hdg'], v['roc'], v['ts']] for v in vs])
    a = np.zeros(vsnp.shape)

    data = []

    for p in ps:
        ts = p['ts']
        icao = p['icao']

        a[:, 0:3] = vsnp[:, 0:3]
        a[:, 3] = np.abs(vsnp[:, 3] - ts)

        b = a[a[:, 3] <= 5]         # v dt within 5 sec
        c = b[b[:, 3].argsort()]    # sort by dt

        if len(c):
            p['spd'] = c[0][0]
            p['hdg'] = c[0][1]
            p['roc'] = c[0][2]
        else:
            p['spd'] = np.nan
            p['hdg'] = np.nan
            p['roc'] = np.nan

        data.append(p)

    mcollmerge.insert(data)
