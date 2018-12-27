# -*- coding: utf8 -*-
# Script to generate a test hint file for use in testing the routines
# that read hintfiles.

# Usage: generate_hintfile.py OUTPUT_PATH

import os
import sys
import time
import struct
import binascii

if sys.version_info.major < 3:
    print('It is impossibly frustrating to get strings right in Python2, '
          'please run with a friendly Python 3')
    sys.exit(1)

records = [
    ('fourty-two', '42'),
    ('three', '3'),
    ('one', '1'),
    ('four', '4'),
    ('something with space', ' '),
    ('two', '2'),
    ('\x00\x01\x5a\x99', '\x00\x00\x1a'),
    ('text key', '\x00\x67'),
    ('\x00\x67', 'text val'),
]

def record_to_bytes(key_bytes, val_bytes, timestamp):
    key_val_bytes = key_bytes + val_bytes
    fmt = '>QHH{}'.format('B' * len(key_val_bytes))

    payload = struct.pack(fmt,
        timestamp,
        len(key_bytes),
        len(val_bytes),
        *key_val_bytes
    )

    checksum = binascii.crc32(payload) & 0xffffffff

    return struct.calcsize('>L' + fmt[1:]), struct.pack('>L', checksum) + payload


def hintfile_record_bytes(timestamp, key_bytes, datafile_payload, offset):
    fmt = '>QHHQ{}'.format('B' * len(key_bytes))

    hint_bytes = struct.pack(fmt,
        timestamp,
        len(key_bytes),
        len(datafile_payload),
        offset,
        *key_bytes
    )

    return struct.calcsize(fmt), hint_bytes


if __name__ == '__main__':
    try:
        output = sys.argv[1]
    except IndexError:
        print('Usage: {} output'.format(sys.argv[0]))
        sys.exit(1)

    datafile = os.path.join(output, '0.data')
    hintfile = os.path.join(output, '0.hint')
    guidefile = os.path.join(output, '0.guide')

    with open(datafile, 'wb') as data_out,\
         open(hintfile, 'wb') as hint_out,\
         open(guidefile, 'wb') as guide_out:

        timestamp = int(time.time())
        offset = 0

        for key, val in records:
            key_bytes = key.encode('utf-8')
            val_bytes = val.encode('utf-8')

            print('writing {}'.format((key_bytes, val_bytes)))

            data_bytes_size, data_bytes =\
                    record_to_bytes(key_bytes, val_bytes, timestamp)

            data_out.write(data_bytes)

            _, hint_bytes = hintfile_record_bytes(timestamp,
                                                  key_bytes,
                                                  data_bytes,
                                                  offset)
            hint_out.write(hint_bytes)

            guide_out.write(",".join(str(field) for field in [
                timestamp,
                0, # file id
                len(key_bytes),
                len(val_bytes),
                offset,
            ]).encode("utf8"))

            guide_out.write(','.encode("utf8"))
            guide_out.write(key_bytes)

            guide_out.write(','.encode("utf8"))
            guide_out.write(val_bytes)

            guide_out.write("\n".encode("utf8"))

            timestamp += 1

            offset += len(key_bytes) + len(val_bytes) + 4 + 8 + 2 + 2;
