import numpy as np
from time import time
from itertools import cycle
from matplotlib import pyplot as plt
from sklearn import preprocessing
from pymongo import MongoClient

#--------------------------------
# Configuration for the database
#--------------------------------
HOST = "localhost"
PORT = 27017
DB = 'SIL'
POS_COLL = '2015-09-08-pos'
SEGMENT_COLL = '2015-09-08-segments'

mongo_client = MongoClient('localhost', 27017)
mdb = mongo_client[DB]

ids = []
lats = []
lons = []
alts = []
times = []

allpos = mdb[POS_COLL].find()

for pos in allpos:
    ids.append(pos['icao'])
    lats.append(pos['loc']['lat'])
    lons.append(pos['loc']['lng'])
    alts.append(pos['alt'])
    times.append(float(pos['ts']))

# slice of the data
# part = int(0.4 * len(ids))

# ids = ids[:part]
# lats = lats[:part]
# lons = lons[:part]
# alts = alts[:part]
# spds = spds[:part]
# times = times[:part]

# transform the text ids into numbers
le = preprocessing.LabelEncoder()
encoded_ids = le.fit_transform(ids)

# scaling the time stamps

# feature scaling - altitude, spds, times
# time step is more significant
mms = preprocessing.MinMaxScaler(feature_range=(0, 1000))   
times_norm = mms.fit_transform(times)
dt = mms.scale_ * 1 * 60 * 60  #time interval of 1 hour
twin = mms.scale_ * 10 * 60

mms = preprocessing.MinMaxScaler(feature_range=(0, 100))
alts_norm = mms.fit_transform(alts)
# ids_norm = mms.fit_transform(map(float, encoded_ids))

## ========================================================
## Clustering - Fight segment extraction
## ========================================================
from sklearn.cluster import Birch, MeanShift, DBSCAN

acs = {}
acsegs = {}

# cluster = Birch(branching_factor=50, n_clusters=None, threshold=10, compute_labels=True)
cluster = DBSCAN(eps=dt, min_samples=50)

for i in xrange(len(ids)):
    if ids[i] not in acs.keys():
        acs[ids[i]] = []
    acs[ids[i]].append([times_norm[i], alts_norm[i], \
        times[i], lats[i], lons[i], alts[i]])

for k in acs.keys():
    data = np.asarray(acs[k])

    tdata = np.copy(data)
    tdata[:, 1:] = 0
    cluster.fit(tdata)
    labels = cluster.labels_
    n_clusters = np.unique(labels).size
    print("n_clusters : %d" % n_clusters)

    # populating the segmented sets
    if k not in acsegs.keys():
        acsegs[k] = []

    for i in range(n_clusters):
        mask = labels == i
        acsegs[k].append(data[mask, 2:6].tolist())

    # # Plot result
    # colorset = cycle(['purple', 'green', 'red', 'blue', 'orange'])
    # for i, c in zip(range(n_clusters), colorset):
    #     mask = labels == i
    #     plt.plot(data[mask, 0], data[mask, 1], 'w', color=c, marker='.', alpha=1.0)
    #     plt.plot(data[mask, 0], data[mask, 2], 'w', color='darkblue', marker='o', alpha=1.0)

    # plt.draw()
    # plt.waitforbuttonpress(-1)
    # plt.clf()


t0 = time()

# clear the segment collection first
mdb[SEGMENT_COLL].drop()

for k, segs in acsegs.iteritems():
    for sg in segs:
        if len(sg) == 0:
            continue
        entry = {'icao': k, 'data':sg}
        mdb[SEGMENT_COLL].save(entry)

print("MongoDB wirte done in %0.3fs" % (time() - t0))
