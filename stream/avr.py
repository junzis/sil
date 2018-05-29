'''Stream AVR RAW format data from a TCP server, convert to mode-s messages'''


import time
from . import BaseStream

class AVRStream(BaseStream):
    def __init__(self, host, port):
        super(AVRStream, self).__init__(host, port)
        self.lines = []
        self.current_msg = ''

    def read_message_in_buffer(self):
        # -- testing --
        # for b in self.buffer:
        #     print(chr(b), b)

        # Append message with 0-9,A-F,a-f, until stop sign

        messages = []
        i = 0

        while i < len(self.buffer):
            b = self.buffer[i]
            if b == 42:
                if len(self.current_msg) in [14, 28]:
                    ts = time.time()
                    messages.append([self.current_msg, ts])

                self.current_msg = ''
            elif 48<=b<=57 or 65<=b<=70 or 97<=b<=102:
                self.current_msg = self.current_msg + chr(b)
            else:
                pass
            i += 1

        self.buffer = []

        return messages
