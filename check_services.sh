#!/bin/bash

PFILE_ADSB=/tmp/sil-adsb-client.pid
PFILE_EHS=/tmp/sil-ehs-client.pid

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

ADSB_SCRIPT=$SCRIPT_DIR/adsb_service_run.py
EHS_SCRIPT=$SCRIPT_DIR/ehs_service_run.py

if [ ! -f $PFILE_ADSB ]
then
	echo "ADS-B client not running, starting now.."
	python $ADSB_SCRIPT start
else
	echo "ADS-B client is running"
fi

if [ ! -f $PFILE_EHS ]
then
	echo "EHS client not running, starting now.."
	python $EHS_SCRIPT start
else
	echo "EHS client is running"
fi
