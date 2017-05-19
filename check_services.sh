#!/bin/bash

PFILE_ADSB=/tmp/beast-adsb-client.pid
PFILE_EHS=/tmp/beast-ehs-client.pid
PFILE_ELS=/tmp/beast-els-client.pid

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

ADSB_SCRIPT=$SCRIPT_DIR/daemon_run_adsb_client.py
EHS_SCRIPT=$SCRIPT_DIR/daemon_run_ehs_client.py
ELS_SCRIPT=$SCRIPT_DIR/daemon_run_els_client.py

if [ ! -f $PFILE_ADSB ]
then
	echo "ADS-B  (DF 17) client not running, starting now.."
	python $ADSB_SCRIPT start
else
	echo "ADS-B  (DF 17) client is running"
fi

if [ ! -f $PFILE_EHS ]
then
	echo "EHS (DF 20/21) client not running, starting now.."
	python $EHS_SCRIPT start
else
	echo "EHS (DF 20/21) client is running"
fi

if [ ! -f $PFILE_ELS ]
then
	echo "ELS (DF 4/5/11) client not running, starting now.."
	python $ELS_SCRIPT start
else
	echo "ELS (DF 4/5/11) client is running"
fi
