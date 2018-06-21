# -*- coding: utf8 -*-
# Script to generate a test hint file for use in testing the routines
# that read hintfiles.

# Usage: generate_hintfile.py OUTPUT_PATH

import sys
import time
import struct

records = {
    'one': '1',
    'two': '2',
    'three': '3',
    'four': '4',
    'fourty-two': '42',
    'something with space': ' ',
}

try:
    output = sys.argv[1]
except IndexError:
    print('Usage: {} output'.format(sys.argv[0]))
    sys.exit(1)

with open(output, 'wb') as fp, open(output+'.guide', 'wb') as gp:
    timestamp = int(time.time())
    offset = 0
    for key, val in sorted(records.items()):
        key_bytes = [ord(byte) for byte in key]
        fmt = '>QHHQ{}'.format('B' * len(key_bytes))
        payload = struct.pack(fmt,
            timestamp,
            len(key),
            len(val),
            offset,
            *key_bytes
        )
        fp.write(payload)
        gp.write(",".join(str(field) for field in
                            (timestamp, len(key), len(val), offset, key)))
        gp.write("\n")
        timestamp += 1
        # That is the real record size in the data file, not important here
        # for testing, but is easy to write, so why not.
        offset += len(key) + len(val) + 4 + 8 + 2 + 2;

