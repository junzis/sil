
import time
import zmq

class BaseStream(object):
    ''' Base class for different stram formats'''
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
                received = [i for i in self.socket.recv(4096)]
                self.buffer.extend(received)

                messages = self.read_message_in_buffer()

                if not messages:
                    continue
                else:
                    self.handle_messages(messages)

                time.sleep(0.0001)
            except Exception as e:
                print("Error:", e)
                self.disconnect()
                time.sleep(3)
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
