#!/usr/bin/python
'''
tia, The Information Adapter
Rodrigo Strauss, 2010

file to container:
    tia -i /tmp/xpto -o tio://127.0.0.1 --separator ^

stdin to container
    tia -o tio://127.0.0.1 --len-prefixed

container to stdout
    tia -i tio://127.0.0.1

container to file
    tia -i tio://127.0.0.1 -o c:\temp\dump.dat

'''

from __future__ import print_function
import sys
import os
import time
from cStringIO import StringIO
from optparse import OptionParser
from pprint import pprint
import struct
import bz2

import tioclient


class LenPrefixed:
    def __init__(self, f):
        self.f = f

    def write(self, data):
        self.f.write(struct.pack('I', len(data)))
        self.f.write(data)
        
    def __iter__(self):
        return LenPrefixedIterator(self.f)

class LenPrefixedIterator:
    def __init__(self, f):
        self.f = f

    def next(self):        
        MAX_RECORD_LEN = 1024 * 1024

        len_buffer = self.f.read(4)

        rec = 0
        d = None

        if len(len_buffer) != 4:
            raise StopIteration()
        
        l = struct.unpack('I', len_buffer)[0]

        return self.f.read(l)

#
# There MUST be something in the python lib that already does this.
# But I didn't find
#
class Separated:
    def __init__(self, f, separator, read_block_size = 1024 * 1024):
        self.f = f
        self.separator = separator
        self.read_block_size = read_block_size
        self.first_write = True

    def write(self, data):
        if self.first_write:
            self.first_write = False
        else:
            self.f.write(self.separator)

        self.f.write(data)            

    def __iter__(self):
        remaining = ''

        while 1:
            block = self.f.read(self.read_block_size)

            if not block:
                if remaining:
                    yield remaining
                return

            if remaining:
                block = remaining + block            
            
            records = block.split(self.separator)

            if records:
                remaining = records[-1]
                del records[-1]            

            # special case to ignore \r in \r\n separated files (Windows text files)
            if self.separator == '\n':
                for x in xrange(len(records)):
                    if records[x][-1] == '\r':
                        records[x] = records[x][:-1]

            for x in records:
                yield x

def open_formatter(f, format, is_stdstream = False):
    if format == 'len_prefixed':
        return LenPrefixed(f)

    # if it's read from stdin, must read byte per byte
    block_size = 1 if is_stdstream else  1024 * 1024

    if format == 'csv':
        return Separated(f, ',', block_size)

    # not format specified, we'll assume separated by \n 
    return Separated(f, '\n', block_size)

def open_stream(url, format, mode, create_container_type):    
    if url.startswith('tio://'):
        return TioEnumerator(tioclient.OpenByUrl(url, create_container_type))

    # From file. Need to check the format
    f = file(url, mode)

    return open_formatter(f, format)
    
def is_tio_stream(s):
    return isinstance(s, tioclient.RemoteDataStore)

class TioEnumerator(object):
    def __init__(self, container):
        self.container = container
        self.manager = self.container.manager
        self.receive_answer_step = 100

    def __del__(self):
        self.manager.ReceivePendingAnswers()

    def write(self, data):
        if not isinstance(data, basestring):
            raise Exception('tia can only write strings to tio containers')
            
        self.container.append(data)

        if self.manager.pendingAnswerCount >= self.receive_answer_step:
            self.manager.ReceivePendingAnswers()

    # TODO: it's slow as hell. Change to subscribe would make it better
    def __iter__(self):
        count = len(self.container)
        for x in xrange(count):
            key, value, metadata = self.container[x]
            if not isinstance(value, basestring):
                raise Exception('tia supports only string values from tio')
            yield value

def SetupCommandLine():
    parser = OptionParser()

    parser.add_option("-i", "--input", dest="input", action='store',
                      help="Input. If not specified, will read from stdin. Use tio://server/container to specify a tio source")

    parser.add_option("-o", "--output", dest="output", action='store',
                      help="Output. If not specified, will write to stdout. Use tio://server/container to specify a tio output")

    parser.add_option("-f", "--input-format", dest="input_format", action='store',
                      help="Specify a input format. [csv], [len_prefixed]")

    parser.add_option("-u", "--output-format", dest="output_format", action='store',
                      help="Specify a output format. [csv], [len_prefixed]")    

    parser.add_option("-e", "--log-each", dest="log_each", action='store',
                      help="log to stderr each N records")

    parser.add_option("-k", "--keys", dest="keys", action='store',
                      help="dump keys instead of values")

    parser.add_option("-r", "--resume", action="store_true", dest="resume", default=False,
                      help="resume load. Skip N input records, where N = destination record count")

    parser.add_option("-g", "--ignore-not-empty", action="store_true", dest="ignore_non_empty", default=False,
                      help="write to tio container even if it's not empty. The default action is abort")

    parser.add_option("-c", "--create-container", action="store", dest="create_container_type", default='',
                      help="create the Tio containers if they don't exist, using the informed type")         

    return parser
    
def main():
    '''
        Do the magic. If no format option in informed, \n is assumed as separator
    '''
    options, args = SetupCommandLine().parse_args()

    #if not options.input and not options.output:
    #    print('missing argument, -h for help')
    #    return -1

    if options.input:
        input = open_stream(options.input, options.input_format, 'rb', options.create_container_type)
    else:
        input = open_formatter(sys.stdin, options.input_format, is_stdstream = True)

    if options.output:
        output = open_stream(options.output, options.output_format, 'wb', options.create_container_type)
    else:
        output = open_formatter(sys.stdout, options.output_format, is_stdstream = True)

    index = -1

    log_each = int(options.log_each) if options.log_each else 1000

    skip_until = 0
    container = getattr(output, 'container', None)

    if options.resume:
        if container is None:
            print('ERROR: option resume requires the output to be a tio container')
            return -1

        skip_until = len(container)
    # if it's a resume operation, we'll not check if it's non empty.
    elif not options.ignore_non_empty and container and len(container) != 0:
        print ('ERROR: destination container is not empty. Use -g (--ignore-non-empty) or -r (--resume)')
        return -1

    #
    # if the destination is a container, we'll disable the answer receive for each command
    # it will make things faster
    #
    if container:
        container.manager.dontWaitForAnswers = True
            
    last_time = time.time()
    
    for index, record in enumerate(input):
        if index >= skip_until:
            output.write(record)

        if (index + 1) % log_each == 0:
            now = time.time()
            elapsed = now - last_time
            last_time = now
            
            if elapsed != 0:
                per_sec = log_each / elapsed
            else:
                per_sec = 123456789.546456

            sys.stderr.write('%d records, %0.2f/s %s\n' % (index + 1, per_sec, '(skipped)' if index < skip_until else ''))

    if index != -1:
        sys.stdout.write('\n%d records total' % (index + 1))
    
    
if __name__ == '__main__':
    sys.exit(main())

    
