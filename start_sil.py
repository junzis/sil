from tendo import singleton
import argparse
from stream.beast import BeastStream
from stream.avr import AVRStream

me = singleton.SingleInstance()

parser = argparse.ArgumentParser()

parser.add_argument(
    "--host",
    dest="host",
    help="hostname or IP address, default:localhost",
    default="127.0.0.1",
)

parser.add_argument("--port", dest="port", help="Network raw stream port")

parser.add_argument(
    "--df-filter",
    dest="df_filter",
    nargs="+",
    help="Downlink formats to save, defualt all: 0 4 5 11 16 17 18 19 20 21 24",
    type=int,
)

parser.add_argument(
    "--type",
    dest="stream_type",
    help="raw stream type, options are: beast or avr",
    default="beast",
)

parser.add_argument(
    "--buffer-size",
    dest="buff_size",
    help="Message buffer size, increas to lower write frequence to disk",
    default=100,
    type=int,
)

parser.add_argument(
    "--output",
    dest="output_dir",
    help="output directory",
    default=None
)

parser.add_argument("--debug", dest="debug", action="store_true", default=False)

args = parser.parse_args()

if args.stream_type.lower() == "beast":
    stream = BeastStream(
        args.host, args.port, df_filter=args.df_filter,
        buff_size=args.buff_size,
        output_dir=args.output_dir
    )

if args.stream_type.lower() == "avr":
    stream = AVRStream(
        args.host, args.port, df_filter=args.df_filter, buff_size=args.buff_size
    )

if args.debug:
    stream.debug = True

stream.receive()
