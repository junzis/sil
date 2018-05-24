from __future__ import print_function
import time
import zmq

class BaseStream(object):
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.buffer = []
        self.socket = None

    def receive(self):
        self.connect()

        print("Reading messages...")
        while True:
            try:
                received = [i for i in self.socket.recv(1024)]
                self.buffer.extend(received)
                # print(''.join(x.encode('hex') for x in self.buffer))

                # process self.buffer when it is longer enough
                # if len(self.buffer) < 2048:
                #     continue
                # -- Removed!! Cause delay in low data rate scenario --

                messages = self.read_message_in_buffer()

                if not messages:
                    continue
                else:
                    self.handle_messages(messages)

                time.sleep(0.001)
            except Exception as e:
                print("Error:", e)
                self.disconnect()
                self.connect()


    def connect(self):
        print("Connecting to server...")
        self.socket = zmq.Context().socket(zmq.STREAM)
        self.socket.setsockopt(zmq.LINGER, 0)
        self.socket.setsockopt(zmq.RCVTIMEO, 2000)
        self.socket.connect("tcp://%s:%s" % (self.host, self.port))

    def disconnect(self):
        print("Disconnecting from server...")
        self.socket.disconnect("tcp://%s:%s" % (self.host, self.port))

    def read_message_in_buffer(self):
        """[Libray code] re-implement this method to read message from buffer"""
        messages = ['messages 1', 'messages 2', 'implement this method.']
        return messages

    def handle_messages(self, messages):
        """[User code] re-implement this method to handle the messages"""
        for msg, ts in messages:
            print("%-28s %f" % (msg, ts))
