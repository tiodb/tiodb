import sys
import tioclient_c
import tioclient
from datetime import datetime

def TEST_tiodata():
    x = TioData()
    x.set(10)

def TEST_connection():
    cn = connect('127.0.0.1', 6666)
    meta = cn.open('meta/containers')
    print meta.get('meta/containers')

    l = cn.create('test_list', 'volatile_list')

    def sink(container, event_name, key, value, metadata):
        print event_name, key, value, metadata

    l.subscribe(sink)
    
    l.clear()
    l.push_back(10)
    l.push_back('asd')
    l.push_back(10.10)

    assert(l.get(0) == l[0] == 10)
    assert(l.get(1) == l[1] == 'asd')
    assert(l.get(2) == l[2] == 10.10)

    print l.query()

def SpeedTest(func, count, bytes, useKey = False):

    v = '*' * bytes

    start = datetime.now()
    mark = start

    log_step = 1000

    for x in xrange(count):
        if useKey:
            func(key=str(x), value=v)
        else:
            func(value=v)
        
        if x % log_step == 0 and x != 0:
            d = datetime.now() - mark
            print "%d, %0.2f/s" % (x, log_step / ((d.seconds * 1000 + d.microseconds / 1000.0) / 1000.0))
            mark = datetime.now()

       
    d = datetime.now() - start
    return count / ((d.seconds * 1000 + d.microseconds / 1000.0) / 1000.0)

def MasterSpeedTest(tio_lib, test_count):
    man = tio_lib.connect('tio://127.0.0.1:6666')

    tests = (
              {'type': 'volatile_map',      'hasKey': True},
              {'type': 'persistent_map',    'hasKey': True},

              {'type': 'volatile_list',      'hasKey': False},
              {'type': 'persistent_list',    'hasKey': False},
            )

    bytes = 30
    namePerfix = datetime.now().strftime('%Y%m%d%H%M%S') + '_'

    for test in tests:
        type = test['type']
        hasKey = test['hasKey']
        ds = man.create(namePerfix + type, type)
        print type
        result = SpeedTest(ds.set if hasKey else ds.push_back, test_count, bytes, hasKey)

        print '%s: %d msg/s' % (type, result)

if __name__ == '__main__':
    test_count = int(sys.argv[1])
    print 'test_count = ', test_count
    print '====================== text protocol ===================='
    MasterSpeedTest(tioclient, test_count)

    print '====================== binary protocol ===================='
    MasterSpeedTest(tioclient_c, test_count)