#!/bin/bash

PFILE_ADSB=/tmp/beast-client.pid

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

SCRIPT=$SCRIPT_DIR/sil_dump_daemon.py

if [ ! -f $PFILE_ADSB ]
then
	echo "ADS-B  (DF 17) client not running, starting now.."
	python3 $SCRIPT start
else
	echo "ADS-B  (DF 17) client is running"
fi
