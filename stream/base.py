import os
import datetime
import csv
import time
import zmq
import pyModeS as pms

dataroot = os.path.dirname(os.path.realpath(__file__)) + "/../data/"


class BaseStream(object):
    """ Base class for different stram formats"""

    def __init__(self, host, port, df_filter=None, buff_size=100,
                 output_dir=None):
        self.host = host
        self.port = port
        self.buffer = []
        self.socket = None

        self.stream_type = None

        self.short_msg_dfs = (0, 4, 5, 11)
        self.long_msg_dfs = (16, 17, 18, 19, 20, 21, 24)
        self.csvbuff = []

        if df_filter is not None:
            self.df_filter = df_filter
        else:
            self.df_filter = self.short_msg_dfs + self.long_msg_dfs

        self.buff_size = buff_size

        self.output_dir = output_dir if output_dir is not None else dataroot
        self.debug = False

    def receive(self):
        self.connect()

        if self.df_filter is not None:
            print("Downlink format filter applied:", self.df_filter)

        print("Receiving messages...")

        while True:
            try:
                received = [i for i in self.socket.recv(4096)]
                self.buffer.extend(received)

                messages = self.read_message_in_buffer()

                if not messages:
                    continue
                else:
                    self.process_messages(messages)

                time.sleep(0.0001)
            except Exception as e:
                time.sleep(3)

    def connect(self):
        print("Connecting to server: {}, port: {}".format(self.host, self.port))
        self.socket = zmq.Context().socket(zmq.STREAM)
        self.socket.setsockopt(zmq.LINGER, 0)
        self.socket.setsockopt(zmq.RCVTIMEO, 2000)
        self.socket.connect("tcp://%s:%s" % (self.host, self.port))

    def disconnect(self):
        print("Disconnecting from server...")
        self.socket.disconnect("tcp://%s:%s" % (self.host, self.port))

    def read_message_in_buffer(self):
        """re-implement this method to read message from buffer"""
        messages = ["messages 1", "messages 2", "implement this method."]
        return messages

    def process_messages(self, messages):
        for msg, ts in messages:
            nstr = len(msg)
            df = pms.df(msg)

            if df not in self.df_filter:
                continue

            if (nstr == 14) and (df not in self.short_msg_dfs):
                continue

            if (nstr == 28) and (df not in self.long_msg_dfs):
                continue

            icao = pms.adsb.icao(msg)
            line = ["%.9f" % ts, "%02d" % df, icao, msg]

            if self.debug:
                print(line)
                continue

            self.csvbuff.append(line)
            if len(self.csvbuff) > self.buff_size:
                # get the current data/hour for the file name
                dh = str(datetime.datetime.utcnow().strftime("%Y%m%d_%H"))
                csv_path = self.output_dir + "RAW_%s.csv" % dh

                with open(csv_path, "a") as fcsv:
                    writer = csv.writer(fcsv)
                    writer.writerows(self.csvbuff)
                self.csvbuff = []
