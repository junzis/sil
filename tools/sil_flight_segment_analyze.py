import numpy as np
from time import time
from itertools import cycle
from matplotlib import pyplot as plt
from sklearn import preprocessing
from pymongo import MongoClient
from pykalman import KalmanFilter
from mpl_toolkits.basemap import Basemap

#--------------------------------
# Configuration for the database
#--------------------------------
HOST = "localhost"
PORT = 27017
DB = 'SIL'
SEGMENT_COLL = '2015-09-08-segments'

mongo_client = MongoClient('localhost', 27017)
mdb = mongo_client[DB]

res = mdb[SEGMENT_COLL].find({})


for r in res:
    data = r['data']
    icao = r['icao']
    
    if len(data) == 0:
        continue

    data = np.asarray(data)

    times = data[:,0]
    lats = data[:,1]
    lons = data[:,2]
    alts = data[:,3]

    # alts_smooth = savitzky_golay(alts, 11, 2)

    # setup mercator map projection.
    plt.subplot(1,2,1)
    m = Basemap(llcrnrlon=-5.,llcrnrlat=45.,urcrnrlon=13.,urcrnrlat=56.,\
                resolution='l',projection='merc')
    m.fillcontinents()
    m.scatter(lons, lats, latlon=True, marker='.', color='green', zorder=10)
    latAMS = 52.3081
    lonAMS = 4.7642
    m.plot(lonAMS, latAMS, latlon=True, marker='o', color='red', zorder=9)
    plt.title('Positions on map')

    plt.subplot(1,2,2)
    # plt.plot(times, alts_smooth, '-', color='red')
    plt.plot(times, alts, '.', color='blue', alpha=0.3)
    plt.ylim([0, 45000])
    plt.title(icao)

    plt.draw()
    plt.waitforbuttonpress(-1)
    plt.clf()