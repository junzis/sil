'''Stream beast binary data from a TCP server, convert to mode-s messages'''

from __future__ import print_function
import time

from . import BaseStream

class BeastStream(BaseStream):
    def __init__(self, host, port):
        super(BeastStream, self).__init__(host, port)
        self.lines = []

    def read_message_in_buffer(self):
        '''
        <esc> "1" : 6 byte MLAT timestamp, 1 byte signal level,
            2 byte Mode-AC
        <esc> "2" : 6 byte MLAT timestamp, 1 byte signal level,
            7 byte Mode-S short frame
        <esc> "3" : 6 byte MLAT timestamp, 1 byte signal level,
            14 byte Mode-S long frame
        <esc> "4" : 6 byte MLAT timestamp, status data, DIP switch
            configuration settings (not on Mode-S Beast classic)
        <esc><esc>: true 0x1a
        <esc> is 0x1a, and "1", "2" and "3" are 0x31, 0x32 and 0x33

        timestamp:
        wiki.modesbeast.com/Radarcape:Firmware_Versions#The_GPS_timestamp
        '''

        messages_mlat = []
        msg = []
        i = 0

        # process the buffer until the last divider <esc> 0x1a
        # then, reset the self.buffer with the remainder

        while i < len(self.buffer):
            if (self.buffer[i:i+2] == [0x1a, 0x1a]):
                msg.append(0x1a)
                i += 1
            elif (i == len(self.buffer) - 1) and (self.buffer[i] == 0x1a):
                # special case where the last bit is 0x1a
                msg.append(0x1a)
            elif self.buffer[i] == 0x1a:
                if i == len(self.buffer) - 1:
                    # special case where the last bit is 0x1a
                    msg.append(0x1a)
                elif len(msg) > 0:
                    messages_mlat.append(msg)
                    msg = []
            else:
                msg.append(self.buffer[i])
            i += 1

        # save the reminder for next reading cycle, if not empty
        if len(msg) > 0:
            reminder = []
            for i, m in enumerate(msg):
                if (m == 0x1a) and (i < len(msg)-1):
                    # rewind 0x1a, except when it is at the last bit
                    reminder.extend([m, m])
                else:
                    reminder.append(m)
            self.buffer = [0x1a] + msg
        else:
            self.buffer = []

        # extract messages
        messages = []
        for mm in messages_mlat:
            msgtype = mm[0]
            # print(''.join('%02X' % i for i in mm))

            if msgtype == 0x32:
                # Mode-S Short Message, 7 byte, 14-len hexstr
                msg = ''.join('%02X' % i for i in mm[8:15])
            elif msgtype == 0x33:
                # Mode-S Long Message, 14 byte, 28-len hexstr
                msg = ''.join('%02X' % i for i in mm[8:22])
            else:
                # Other message tupe
                continue

            if len(msg) not in [14, 28]:
                # incomplete message
                continue

            ts = time.time()

            messages.append([msg, ts])
        return messages
