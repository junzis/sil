import sys
import os
import client
from daemon import runner

f = open(os.devnull, 'w')
sys.stdout = f

app = client.Client()
daemon_runner = runner.DaemonRunner(app)
daemon_runner.do_action()
