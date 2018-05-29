import os
import datetime
import csv
import pyModeS as pms

import daemon
from daemon import pidfile

# from stream.beast import BeastStream
from stream.avr import AVRStream

dataroot = os.path.dirname(os.path.realpath(__file__)) + "/data/"

class SilStream(AVRStream):
    def __init__(self, host, port):
        super(SilStream, self).__init__(host, port)

        self.adsb_rows = []
        self.ehs_rows = []
        self.els_rows = []

    def handle_messages(self, messages):
        # get the current date file
        today = str(datetime.datetime.now().strftime("%Y%m%d"))
        adsb_csv_path = dataroot + 'ADSB_RAW_%s.csv' % today
        ehs_csv_path = dataroot + 'EHS_RAW_%s.csv' % today
        els_csv_path = dataroot + 'ELS_RAW_%s.csv' % today

        for msg, ts in messages:
            if len(msg) not in [14, 28]:
                continue

            df = pms.df(msg)
            icao = pms.adsb.icao(msg)

            if df==17:
                tc = pms.adsb.typecode(msg)
                line = ['%.6f'%ts, icao, '%02d'%tc, msg]
                self.adsb_rows.append(line)
            elif df==20 or df==21:
                line = ['%.6f'%ts, icao, msg]
                self.ehs_rows.append(line)
            elif df==4 or df==5 or df==11:
                line = ['%.6f'%ts, icao, msg]
                self.els_rows.append(line)

        if len(self.adsb_rows) > 1000:
            with open(adsb_csv_path, 'a') as adsb_fcsv:
                writer = csv.writer(adsb_fcsv)
                writer.writerows(self.adsb_rows)
            self.adsb_rows = []

        if len(self.ehs_rows) > 1000:
            with open(ehs_csv_path, 'a') as ehs_fcsv:
                writer = csv.writer(ehs_fcsv)
                writer.writerows(self.ehs_rows)
            self.ehs_rows = []

        if len(self.els_rows) > 1000:
            with open(els_csv_path, 'a') as els_fcsv:
                writer = csv.writer(els_fcsv)
                writer.writerows(self.els_rows)
            self.els_rows = []

if __name__ == '__main__':
    HOST = "127.0.0.1"

    # PORT = 30334    # Beast
    PORT = 30003    # AVR

    stream = SilStream(host=HOST, port=PORT)
    # stream.receive()      # for test

    fpid = '/tmp/beast-client.pid'
    context = daemon.DaemonContext(
        pidfile=pidfile.TimeoutPIDLockFile(fpid),
    )
    with context:
        stream.receive()
