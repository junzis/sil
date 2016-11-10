import os
import datetime
import csv
import pyModeS as pms
from daemon.runner import DaemonRunner

from client import BaseClient

dataroot = os.path.dirname(os.path.realpath(__file__)) + "/data/"

HOST = "127.0.0.1"
PORT = 30334

class EHSClient(BaseClient):
    def __init__(self, host, port):
        super(EHSClient, self).__init__(host, port)
        self.stdin_path = '/dev/null'
        self.stdout_path = '/tmp/ehs-stdout.log'
        self.stderr_path = '/tmp/ehs-error.log'
        self.pidfile_path = '/tmp/beast-ehs-client.pid'
        self.pidfile_timeout = 5

    def handle_messages(self, messages):
        # get the current date file
        today = str(datetime.datetime.now().strftime("%Y%m%d"))
        csvfile = dataroot + 'EHS_RAW_%s.csv' % today

        with open(csvfile, 'a') as f:
            writer = csv.writer(f)
            for msg, ts in messages:
                if len(msg) < 28:
                    continue

                df = pms.df(msg)

                if df not in [20, 21]:
                    continue

                addr = pms.ehs.icao(msg)

                line = ['%.6f'%ts, addr, msg]

                writer.writerow(line)

if __name__ == '__main__':
    app = EHSClient(host=HOST, port=PORT)
    daemon_runner = DaemonRunner(app)
    daemon_runner.do_action()
