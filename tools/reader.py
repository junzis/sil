""" 
Decoding aircraft data from ADSM message database

"""

import sys
import time
import logging
import threading
import pymongo
from matplotlib import pyplot as plt

sys.path.append('../adsb_decoder/')
import decoder

#--------------------------------
# Configuration for the database
#--------------------------------
HOST = "localhost"
PORT = 27017
DB = 'SIL'

def worker(s, icaos, msgcoll, poscoll, vhcoll):
    '''Decode positions and velocity for an aircraft'''
    
    # waiting for the thread semaphore
    with s:
        logging.debug('Thread started.')
        tic = time.time()

        #------------------------------------------
        # decode postions for the set of aircrafts
        #------------------------------------------
        posmsgs = msgcoll.find({'addr':{'$in':icaos}, 'tc':{'$lt':19, '$gt':8}})
        
        pool = {}
        for icao in icaos:
            pool[icao] = {'msg':[], 'oe':[], 'ts':[]}

        for pm in posmsgs:
            addr = pm['addr']
            msg = pm['msg']
            ts = pm['time']
            pool[addr]['msg'].append( msg )
            pool[addr]['oe'].append( int(decoder.get_oe_flag(msg)) )
            pool[addr]['ts'].append( int(ts) )

        positions = []

        for icao, data in pool.iteritems():
            zipped = zip(data['oe'], data['msg'], data['ts'])
            d0 = d1 = None

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
                        positions.append({'icao':icao, \
                            'loc': {'lat':pos[0], 'lng':pos[1]}, \
                            'alt':alt, 'ts':t})

        # insert records into MongoDB
        if positions:
            poscoll.insert(positions)

        #------------------------------------------
        # decode velocity and headings
        #------------------------------------------
        velocities = []
        velomsgs = msgcoll.find({'addr':{'$in':icaos}, 'tc':19})
        for vm in velomsgs:
            addr = vm['addr']
            [spd, hdg] = decoder.get_speed_heading(vm['msg'])
            t = int(vm['time'])
            velocities.append({'icao':addr, 'spd':spd, 'hdg':hdg, 'ts':t})

        # insert records into MongoDB
        if velocities:
            vhcoll.insert(velocities)

        toc = time.time()
        tt = int(toc - tic)

        logging.debug( str(tt) + ' seconds. ' \
                     + str(len(positions)) + ' postions, ' \
                     + str(len(velocities)) + ' velocities.' )
    return


def main():
    # check script arguments - colletion name
    args = sys.argv
    if len(args) < 2:
        sys.exit("MongoDB collection not specified..")

    # Connect to MongoDB
    mclient = pymongo.MongoClient(HOST, PORT)
    mdb = pymongo.database.Database(mclient, DB)
    collname = args[1]
    msgcoll = mdb[collname]
    poscoll = mdb[collname + '-pos']
    vhcoll = mdb[collname + '-vh']


    # Get all aircrafts and then decode positions and velocities of each
    # find all the ICAO ID we have seen
    stats = msgcoll.aggregate([
        {
            '$group':{
                '_id':'$addr', 
                'count':{'$sum':1}
            }
        }
    ])

    icaos = []

    # get a list of ICAO ids that to be processed
    for ac in stats['result']:
        if (ac['count'] > 500):
            icaos.append(ac['_id'])

    # Threading
    logging.basicConfig(level=logging.DEBUG, 
            format='%(asctime)s (%(threadName)-2s) %(message)s')

    # launching a pool of threads for a chunk of icaos
    # chuck will increase the query spead a LOT!!
    chunk_size = 50
    n_chunks = int ( len(icaos) / chunk_size ) + 1

    print 'launching %d threads for processing aircrafts' % (n_chunks)

    s = threading.Semaphore(6)
    threads = []
    for i in xrange(n_chunks):
        icao_chunk = icaos[i*chunk_size : (i+1)*chunk_size]
        t =  threading.Thread(target=worker, name=i, 
                args=(s, icao_chunk, msgcoll, poscoll, vhcoll))
        t.setDaemon(True)
        t.start()
        threads.append(t)

    # attach all children thread to main thread
    for t in threads:
        t.join()

if __name__ == '__main__':
    tic = time.time()
    
    main()

    toc = time.time()
    tt = (toc-tic)/60
    print 'Completed. Total processing time: %.2f minutes.' % (tt)