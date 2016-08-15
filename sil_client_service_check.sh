#!/bin/bash

PFILE=/tmp/sil-adsb-client.pid
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PY_SCRIPT=$SCRIPT_DIR/sil_client_service.py

echo $PY_SCRIPT

if [ ! -f $PFILE ]
then
	echo "adsb client not running, starting now.."
	python $PY_SCRIPT start
else
	echo "adsb client is running"
fi
