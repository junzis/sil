#!/bin/bash

PFILE_ADSB=/tmp/beast-adsb-client.pid
PFILE_EHS=/tmp/beast-ehs-client.pid

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

ADSB_SCRIPT=$SCRIPT_DIR/daemon_run_adsb_client.py
EHS_SCRIPT=$SCRIPT_DIR/daemon_run_ehs_client.py

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
