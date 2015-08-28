import os
import sys
import subprocess

if not os.path.isfile('/tmp/sil-adsb-client.pid'):
	subprocess.call([sys.executable, 'sil-client-service.py', 'start'])

