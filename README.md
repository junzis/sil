# SIL - C&S ADS-B Collector Client Application

The software is designed to collect raw ADS-B message from C&S antenna Readbeast server, and store them in MongoDB. Codes located in ```tools``` are to be used on ADS-B message processing, which are not fully tested.

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


Check both service status, and start if not running:
```sh
$ python check_service.py
```
