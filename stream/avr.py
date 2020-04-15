"""Stream AVR RAW format data from a TCP server, convert to mode-s messages"""

import time
from .base import BaseStream


class AVRStream(BaseStream):
    def __init__(self, host, port, df_filter=None, buff_size=100):
        super(AVRStream, self).__init__(host, port, df_filter, buff_size)
        self.lines = []
        self.current_msg = ""

    def read_message_in_buffer(self):
        # -- testing --
        # for b in self.buffer:
        #     print(chr(b), b)

        # Append message with 0-9,A-F,a-f, until stop sign

        messages = []

        msg_stop = False
        for b in self.buffer:
            if b == 59:
                msg_stop = True
                ts = time.time()
                messages.append([self.current_msg, ts])
            if b == 42:
                msg_stop = False
                self.current_msg = ""

            if (not msg_stop) and (48 <= b <= 57 or 65 <= b <= 70 or 97 <= b <= 102):
                self.current_msg = self.current_msg + chr(b)

        self.buffer = []

        return messages
