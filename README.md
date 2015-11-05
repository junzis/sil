# SIL - C&S ADS-B Collector Client Application

The software is designed to collect raw ADS-B message from C&S antenna Readbeast server, and store them in MongoDB. Codes located in ```tools``` are to be used on ADS-B message processing, which are not fully tested.

## Runing Client Service
Start the client service with:
```sh
$ python sil_client_service.py start
```

Stop the client service with:
```sh
$ python sil_client_service.py stop
```


Check the service, and start existing:
```sh
$ python check_service.py
```

## Extra: How to dump and restore MongoDB
To dump your database for backup you call this command on your terminal
```sh
$ mongodump --db database_name --collection collection_name
```

Or dump directly to a gzipped file
```sh
$ mongodump --db database_name --collection collection_name --out - | gzip > dump_file_name.gz
```

To import your backup file to mongodb you can use the following command on your
terminal
```sh
$ mongorestore --db database_name path_to_bson_file.bson
```

Extract an GZIP file with:
```
gunzip -c dump_file_name.gz > dump_file_name.bson  
```