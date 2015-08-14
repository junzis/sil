""" 
Stream data from a TCP server providing datafeed of ADS-B messages

"""

import os
import socket
import time
import threading
import math
from pymongo import MongoClient
from adsb_decoder import decoder

# mongo_client = MongoClient('localhost', 27017)
# adsb_collection = mongo_client.ADSB.messages0703

host = '131.180.117.39'
port = 10001
tcp_buffer_size = 1024


def read_message(msgtype, data):
    ''' Process the message that received from remote TCP server '''  
    if msgtype == 3:
        # get time second
        tv_sec = 0
        tv_sec |= data[0] << 24
        tv_sec |= data[1] << 16
        tv_sec |= data[2] << 8
        tv_sec |= data[3]

        # get time nano-second
        tv_nsec = 0
        tv_nsec |= data[4] << 24
        tv_nsec |= data[5] << 16
        tv_nsec |= data[6] << 8
        tv_nsec |= data[7]

        timestamp = float( str(tv_sec) + '.' + str(tv_nsec) )

        # ignor receiver id, data[8]
        # ignor data[9], for now

        mlat = 0
        mlat |= data[10] << 24
        mlat |= data[11] << 16
        mlat |= data[12] << 8
        mlat |= data[13]

        power = 0
        power |= data[14] << 8
        power |= data[15]
        power &= 0x3FFF  
        power = power >> 6

        # process msg in the data frame
        msg = ''
        msglen = 14     # type 3 data length is 14
        msgstart = 16
        msgend = msgstart + msglen
        for i in data[msgstart : msgend] :
            msg += "%02X" % i

        # print "Type:%d | Time: %f | Power:%d | MSG: %s"  % (msgtype, timestamp, power, msg)

        df = decoder.get_df(msg)

        print df

        return

def connect():
    while True:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(10)    # 10 second timeout
            s.connect((host, port))
            # print "Server %s connected" % host
            return s
        except socket.error as err:
            # print "Socket connection error: %s. reconnecting..." % err
            time.sleep(3)

sock = connect()

while True:
    try:
        raw_data = sock.recv(tcp_buffer_size)
        if raw_data == b'':
            raise RuntimeError("socket connection broken")
        else:
            # print ''.join(x.encode('hex') for x in raw_data)

            data = [ord(i) for i in raw_data]    # covert the char to int

            if data[0] == 27:                    # looking for ADS-B data, start with "0x1B"
                try:
                    datatype = int(raw_data[1])
                    datalen = data[2]<<8 | data[3]
                    read_message(datatype, data[4:])
                except Exception, err:
                    # print err
                    pass
    except RuntimeError, err:
        # print "Error %s. reconnecting.." % err
        sock = connect()
        pass

