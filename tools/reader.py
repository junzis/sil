""" 
Decoding aircraft data from ADSM message database

"""

import time
from pymongo import MongoClient
from adsb_decoder import decoder
from matplotlib import pyplot as plt

mongo_client = MongoClient('localhost', 27017)
msg_collection = mongo_client.ADSB.messages0707
pos_collection = mongo_client.ADSB.pos_0707
vh_collection = mongo_client.ADSB.vh_0707

res = msg_collection.aggregate([
    {
        '$group':{
            '_id':'$addr', 
            'count':{'$sum':1}
        }
    }
])

icao_count = len(res['result'])
print str(icao_count) + ' number of aircrafts found'

for ac in res['result']:
    print 'processing aircraft: ' + ac['_id']

    # only process aircraft with decent number of messages
    if(ac['count'] < 300):
        print 'not enough data, ignored.'
        print ''
        continue;

    # decode the positions
    posmsgs = msg_collection.find({'addr':ac['_id'], 'tc':{'$lt':19, '$gt':8}})
    msgpool = []
    oepool = []
    tspool = []
    for pm in posmsgs:
        msgpool.append(pm['msg'])
        oepool.append( int(decoder.get_oe_flag(pm['msg'])) )
        tspool.append( int(pm['time']) )

    zipped = zip(oepool, msgpool, tspool)
    positions = []
    d0 = d1 = False

    for d in zipped:
        if d[0] == 0:
            d0 = d
        if d[0] == 1:
            d1 = d
        # check if timestamp is too far away
        if d0 and d1 and abs(d1[2] - d0[2]) < 5:
            pos = decoder.get_position(d0[1], d1[1], d0[2], d1[2])
            alt = decoder.get_alt(d[1])
            t = d[2]
            # only add position if it can be calculated
            if pos:
                positions.append({'icao':ac['_id'], \
                    'loc': {'lat':pos[0], 'lng':pos[1]}, \
                    'alt':alt, 'ts':t})

    # insert records into MongoDB
    if positions:
        pos_collection.insert(positions)

    # decode velocity and headings
    velomsgs = msg_collection.find({'addr':ac['_id'], 'tc':19})
    velocities = []
    for vm in velomsgs:
        [spd, hdg] = decoder.get_speed_heading(vm['msg'])
        t = int(vm['time'])
        velocities.append({'icao':ac['_id'], 'spd':spd, 'hdg':hdg, 'ts':t})

    # insert records into MongoDB
    if velocities:
        vh_collection.insert(velocities)

    print str(len(positions)) + ' positions and ' \
            + str(len(velocities)) + ' velocities recoded.'
    print ''
    
    # TODO: merge velocity and positions


# # Compare position and velocity time stamps
# t1 = zip(*positions)[2]
# t2 = zip(*velocities)[2]
# plt.plot(t1, [1]*len(t1), 'x')
# plt.plot(t2, [2]*len(t2), 'x')
# plt.ylim([0,3])
# plt.show()

# # save as KML to test
# import simplekml
# kml = simplekml.Kml()
# coords = []
# for p in positions[2000:3000]:
#     coords.append((p[0][1], p[0][0], int(p[1]*0.3048)))
# ls = kml.newlinestring()
# ls.coords = coords
# ls.extrude = 1
# ls.altitudemode = simplekml.AltitudeMode.absolute
# ls.style.linestyle.color = simplekml.Color.blue 
# ls.style.linestyle.width = 2  # 2 pixels
# kml.save("test.kml")