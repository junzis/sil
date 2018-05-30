import argparse
from stream.beast import BeastStream
from stream.avr import AVRStream

parser = argparse.ArgumentParser()
parser.add_argument('--debug', dest='debug', action='store_true', default=False)
parser.add_argument('--type', dest='stream_type', help="raw stream type [beast/avr]", default='avr')
args = parser.parse_args()

host = "127.0.0.1"

if args.stream_type.lower() == 'beast':
    port = 30334
    stream = BeastStream(host, port)

if args.stream_type.lower() == 'avr':
    port = 30003
    stream = AVRStream(host, port)

if args.debug:
    stream.debug = True

stream.receive()
