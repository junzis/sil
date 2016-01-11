"""
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
parser.add_argument('--collpos', dest='collpos', required=True,
                    help="Postion collection name")
parser.add_argument('--collvh', dest='collvh', required=True,
                    help="Postion collection name")
parser.add_argument('--collmerge', dest='collmerge', required=True,
                    help="Postion collection name")
args = parser.parse_args()

db = args.db
collpos = args.collpos
collvh = args.collvh
collmerge = args.collmerge

mclient = MongoClient('localhost', 27017)

mcollpos = mclient[db][collpos]
mcollvh = mclient[db][collvh]
mcollmerge = mclient[db][collmerge]

# clear output database
mcollmerge.drop()

# fetch all icaos
q = mcollpos.aggregate([
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
    print "processing %d of %d" % (count, total_counts)

    positions = list(mcollpos.find({'icao': icao}))
    vhs = list(mcollvh.find({'icao': icao}))

    if not len(vhs):
        continue

    vhsnp = np.array([[vh['spd'], vh['hdg'], vh['ts']] for vh in vhs])
    a = np.zeros(vhsnp.shape)

    data = []

    for pos in positions:
        ts = pos['ts']
        icao = pos['icao']

        a[:, 0] = vhsnp[:, 0]
        a[:, 1] = vhsnp[:, 1]
        a[:, 2] = np.abs(vhsnp[:, 2] - ts)

        b = a[a[:, 2] <= 5]
        c = b[b[:, 2].argsort()]

        if len(c):
            pos['spd'] = c[0][0]
            pos['hdg'] = c[0][1]
        else:
            pos['spd'] = np.nan
            pos['hdg'] = np.nan

        data.append(pos)

    mcollmerge.insert(data)
