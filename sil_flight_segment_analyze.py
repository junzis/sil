import numpy as np
from time import time
from itertools import cycle
from matplotlib import pyplot as plt
from sklearn import preprocessing
from pymongo import MongoClient
from pykalman import KalmanFilter
from mpl_toolkits.basemap import Basemap


mongo_client = MongoClient('localhost', 27017)
mdb = mongo_client.ADSB

res = mdb.segments.find({})

def runningMeanFast(x, N):
    return np.convolve(x, np.ones((N,))/N, mode="valid")

def savitzky_golay(data, window_size, order, deriv=0, rate=1):
    import numpy as np
    from math import factorial

    if window_size % 2 != 1 or window_size < 1:
        window_size = window_size + 1
    if window_size < order + 2:
        window_size = order + 2
    order_range = range(order+1)
    half_window = (window_size -1) // 2
    # precompute coefficients
    b = np.mat([[k**i for i in order_range] for k in range(-half_window, half_window+1)])
    m = np.linalg.pinv(b).A[deriv] * rate**deriv * factorial(deriv)
    # pad the signal at the extremes with
    # values taken from the signal itself
    firstvals = data[0] - np.abs( data[1:half_window+1][::-1] - data[0] )
    lastvals = data[-1] + np.abs(data[-half_window-1:-1][::-1] - data[-1])
    data = np.concatenate((firstvals, data, lastvals))
    return np.convolve( m[::-1], data, mode='valid')

def movingaverage(interval, window_size):
    import numpy as np
    window = np.ones(int(window_size))/float(window_size)
    return np.convolve(interval, window)

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

    alts_smooth = savitzky_golay(alts, 11, 2)

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
    plt.plot(times, alts_smooth, '-', color='red')
    plt.plot(times, alts, '.', color='blue', alpha=0.3)
    plt.ylim([0, 45000])
    plt.title(icao)

    plt.draw()
    plt.waitforbuttonpress(-1)
    plt.clf()