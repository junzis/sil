# Python Client for Mode S Raw Data

The tool is designed in Python to dump raw ADS-B and Mode S message from a beast or AVR data stream.

## 1. Install dependencies

```sh
$ pip install pyModeS tendo pandas
```

## 2. Collecting data

### [optional] Setting up the source (if you don't have a raw stream)

Install [dump1090](https://github.com/flightaware/dump1090) and run following:

```ssh
$ ./dump1090 --net --quiet
```

Now you will have the raw messages served on TCP ports 30002 (AVR format) and 30005 (beast format). You can check using ``telnet`` command.

```sh
$ telnet 127.0.0.1 30002
$ telnet 127.0.0.1 30005
```

### Saving raw data using SIL script

Once you have a TCP raw message stream ready, use `start_sil.py` from this repository to collect data. First download this repository, or clone it use:

```ssh
$ git clone https://github.com/junzis/sil.git
$ cd sil
```

Then you can starting save raw messages.

For example, collecting ADS-B only from a AVR stream:

```ssh
$ python start_sil.py --port 30002 --type avr --df-filter 17
```

or, collecting multiple DF used Mode S Beast stream from a remote server

```ssh
$ python start_sil.py --host [hostname_or_ip] --port 30005 --type beast --df-filter 17 20 21
```

Additional information

- User `python start_sil.py -h` to see more options
- Option `--df-filter` allows you to specify the Downlink Formats to save
- Increase `--buffer-size` to decrease the frequency of saving data to disk
- Data is saved per hour (UTC), under `data` folder, with format `RAW_YYYYMMDD_HH.csv`
- CSV columns are: unix timestamp, downlink format, ICAO address, raw message


## 3. Start the script automatically

To start the script automatically, add the following to ``contrab``:

```
@reboot python3 \[path_to_script]\start_sil.py [options] &
```

## 4. Decoding save data

You can decode saved data using the scripts under `extra_tools`. For example:

```sh
$ python extra_tools/decode_adsb_single_thread.py --fin [input_file] --fout [output_file] --lat0 [receiver_latitude] --lon0 [longitude]
```
