# Python Client for Mode-s Beast Raw Data

The tool is designed in Python to dump raw ADS-B or EHS message from a mode-s beast raw data stream.

For example, it can be used in combination with [modesmixer2](http://xdeco.org/?page_id=48) and your favorite receiver:

```sh
$ modesmixer2 --inSeriel port[:speed[:flow_control]] --outServer beast:30334
```

Now you will have the RAW message served on TCP 30334 port. You can check with ``telnet`` command.

```sh
$ telnet 127.0.0.1 30334
```

## Extend the code to an ADS-B streaming application

Here is an example to use ``stream.base.BaseClient()`` class for you own ADS-B online processing.

```python
import pyModeS as pms
from stream.base import BaseClient

# define your custom class extending the BaseClient
#   - implement your handle_messages() methods
class ADSBClient(BaseClient):
    def __init__(self, host, port):
        super(ADSBClient, self).__init__(host, port)

    def handle_messages(self, messages):
        for msg, ts in messages:
            if len(msg) < 28:           # wrong data length
                continue

            df = pms.df(msg)

            if df != 17:                # not ADSB
                continue

            if '1' in pms.crc(msg):     # CRC fail
                continue

            icao = pms.adsb.icao(msg)
            tc = pms.adsb.typecode(msg)

            # TODO: write you magic code here
            print ts, icao, tc, msg

client = ADSBClient(host='127.0.0.1', port=30334)
client.run()

```

## Runing Client Service as daemon
Start the client (ADSB, EHS, or ELS) service with:
```sh
$ python daemon_run_adsb_client.py start
$ python daemon_run_ehs_client.py start
$ python daemon_run_ehs_client.py start
```

Raw messages will be saved to following plain text files, and sorted by date:

```
./data/ADSB_RAW_YYYYMMDD.csv
./data/EHS_RAW_YYYYMMDD.csv
./data/ELS_RAW_YYYYMMDD.csv
```


Stop the client service with:
```sh
$ python daemon_run_adsb_client.py stop
$ python daemon_run_ehs_client.py stop
$ python daemon_run_els_client.py stop
```


Script to check all service status, and start them if not running:

```sh
$ python check_service.py
```
