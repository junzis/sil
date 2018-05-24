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

class EHSStream(BeastStream):
    def __init__(self, host, port):
        super(EHSStream, self).__init__(host, port)
        self.stdin_path = '/dev/null'
        self.stdout_path = '/tmp/ehs-stdout.log'
        self.stderr_path = '/tmp/ehs-error.log'
        self.pidfile_path = '/tmp/beast-ehs-client.pid'
        self.pidfile_timeout = 5

        self.lines = []

    def handle_messages(self, messages):
        # get the current date file
        today = str(datetime.datetime.now().strftime("%Y%m%d"))
        csvfile = dataroot + 'EHS_RAW_%s.csv' % today

        for msg, ts in messages:
            if len(msg) < 28:
                continue

            df = pms.df(msg)

            if df not in [20, 21]:
                continue

            addr = pms.ehs.icao(msg)

            line = ['%.6f'%ts, addr, msg]

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
    stream = EHSStream(host=HOST, port=PORT)
    daemon_runner = DaemonRunner(stream)
    daemon_runner.do_action()
