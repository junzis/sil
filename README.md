# Python Client for Mode-s Beast Raw Data

The tool is designed in Python to dump raw ADS-B or EHS message from a mode-s beast raw data stream.

For example, it can be used in combination with [modesmixer2](http://xdeco.org/?page_id=48) and your favorite receiver:
```sh
$ modesmixer2 --inSeriel port[:speed[:flow_control]] --outServer beast:30334
```

## Runing Client Service
Start the client (ADSB or EHS) service with:
```sh
$ python daemon_run_adsb_client.py start
$ python daemon_run_ehs_client.py start
```

Stop the client service with:
```sh
$ python daemon_run_adsb_client.py stop
$ python daemon_run_ehs_client.py stop
```


Script quickly checking both service status, start them if not running:
```sh
$ python check_service.py
```
