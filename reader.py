""" 
Decoding aircraft data from ADSM message database

"""

import time
from pymongo import MongoClient
from adsb_decoder import decoder

mongo_client = MongoClient('localhost', 27017)
adsb_collection = mongo_client.ADSB.messages0707

# res = adsb_collection.aggregate([
#     {
#         '$group':{
#             '_id':'$addr', 
#             'count':{'$sum':1}
#         }
#     }
# ])

# icao_count = len(res['result'])
# print icao_count + ' number of aircrafts found'

# for ac in res['result']:
#     # only process aircraft with decent number of messages
#     if(ac['count'] < 1000):
#         continue;

# decode the positions
# subres = adsb_collection.find({'addr':ac['_id'], 'tc':{'$lt':19, '$gt':8}})
subres = adsb_collection.find({'addr':'4841DB', 'tc':{'$lt':19, '$gt':8}})
msgpool = []
oepool = []
tspool = []
for sr in subres:
    msgpool.append(sr['msg'])
    oepool.append( int(decoder.get_oe_flag(sr['msg'])) )
    tspool.append(sr['time'])