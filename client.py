""" 
Stream data from a TCP server providing datafeed of ADS-B messages

"""

import os
import socket
import time
import datetime
import math
from pymongo import MongoClient
from adsb_decoder import decoder

class Client():

    def __init__(self):
        self.stdin_path = '/dev/null'
        self.stdout_path = '/dev/tty'
        self.stderr_path = '/dev/tty'
        self.pidfile_path =  '/tmp/sil-adsb-client.pid'
        self.pidfile_timeout = 5

    def extract_adsb(self, data):
        ''' Process the message that received from remote TCP server '''  
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

        # receiver power
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

        df = decoder.get_df(msg)

        if df == 17:
            addr = decoder.get_icao_addr(msg)
            tc = decoder.get_tc(msg)
            adsb = {}
            adsb['addr'] = addr
            adsb['power'] = power
            adsb['msg'] = msg
            adsb['tc'] = tc
            adsb['time'] = timestamp
            return adsb
        return None

    def connect(self, host, port):
        while True:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(10)    # 10 second timeout
                s.connect((host, port))
                print "Server %s connected" % host
                return s
            except socket.error as err:
                print "Socket connection error: %s. reconnecting..." % err
                time.sleep(3)

    def run(self):
        mclient = MongoClient('localhost', 27017)
        mdb = mclient.SIL

        host = '127.0.0.1'
        port = 10001
        tcp_buffer_size = 1024

        sock = self.connect(host, port)

        while True:
            try:
                raw_data = sock.recv(tcp_buffer_size)
                if raw_data == b'':
                    raise RuntimeError("socket connection broken")
                else:
                    # print ''.join(x.encode('hex') for x in raw_data)

                    # covert the char to int
                    data = [ord(i) for i in raw_data]

                    # looking for ADS-B data, start with "0x1B", or 27
                    if data[0] != 27:
                        pass

                    msgtype = int(raw_data[1])
                    msglen = data[2]<<8 | data[3]
                    msgsegment = data[4:]

                    # check message type
                    if msgtype == 3:
                        pass;

                    # get adsb data to be recorded
                    adsb = self.extract_adsb(msgsegment)
                    if adsb:
                        # print adsb
                        coll_name = str(datetime.date.today())
                        mcoll = mdb[coll_name]
                        mcoll.insert(adsb)
            except RuntimeError, e:
                print "Error:", e
                print "Socket reconnecting..."
                sock = connect(host, port)
                pass
            except Exception, e:
                print "Unexpected Error:", e
                pass

if __name__ == '__main__':
    client = Client()
    client.run()