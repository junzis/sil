import os
import datetime
import csv
import pyModeS as pms
from daemon.runner import DaemonRunner

from stream.beast import BeastStream
from stream.raw import RawStream

dataroot = os.path.dirname(os.path.realpath(__file__)) + "/data/"

HOST = "127.0.0.1"
PORT = 30334

class ADSBStream(BeastStream):
    def __init__(self, host, port):
        super(ADSBStream, self).__init__(host, port)
        self.stdin_path = '/dev/null'
        self.stdout_path = '/tmp/adsb-stdout.log'
        self.stderr_path = '/tmp/adsb-error.log'
        self.pidfile_path = '/tmp/beast-adsb-client.pid'
        self.pidfile_timeout = 5

        self.lines = []

    def handle_messages(self, messages):
        # get the current date file
        today = str(datetime.datetime.now().strftime("%Y%m%d"))
        csvfile = dataroot + 'ADSB_RAW_%s.csv' % today

        for msg, ts in messages:
            if len(msg) < 28:
                continue

            df = pms.df(msg)

            if df != 17:
                continue

            if '1' in pms.crc(msg):
                continue

            addr = pms.adsb.icao(msg)
            tc = pms.adsb.typecode(msg)

            line = ['%.6f'%ts, addr, '%02d'%tc, msg]

            self.lines.append(line)

        if len(self.lines) > 1000:
            try:
                fcsv = open(csvfile, 'a')
                writer = csv.writer(fcsv)
                writer.writerows(self.lines)
                fcsv.close()
            except Exception, err:
                print err

            self.lines = []

if __name__ == '__main__':
    stream = ADSBStream(host=HOST, port=PORT)
    daemon_runner = DaemonRunner(stream)
    daemon_runner.do_action()
