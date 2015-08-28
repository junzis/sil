import os
import sys
import subprocess

if not os.path.isfile('/tmp/sil-adsb-client.pid'):
	subprocess.call([sys.executable, os.path.dirname(os.path.realpath(__file__))+'/sil-client-service.py', 'start'])

