import intelihubclient
import random
import unittest
import datetime
import uuid
import sys

class ListReceiveCounter(object):
    def __init__(self, test_case):
        self.test_case = test_case
        self.receive_count = 0
        self.last_received_tuple = None
    def on_event(self, container, event_name, k, v, m):
        self.test_case.assertEqual(v, self.receive_count)
        self.last_received_tuple = (k, v, m)
        self.receive_count += 1

class InteliHubTestCase(unittest.TestCase):
    def setUp(self):
        self.hub = intelihubclient.connect('tio://127.0.0.1')

    def get_me_a_random_container_name(self):
        return uuid.uuid4().hex


class PerfomanceTests(InteliHubTestCase):
    def __speed_test(self, func, count, bytes, useKey = False):
        v = '*' * bytes
        
        mark = datetime.datetime.now()

        log_step = 2000

        for x in xrange(count):
            if useKey:
                func(key=str(x), value=v)
            else:
                func(v)
                
            if x % log_step == 0 and x != 0:
                d = datetime.datetime.now() - mark
                print "%d, %0.2f/s   " % (x, log_step / ((d.seconds * 1000 + d.microseconds / 1000.0) / 1000.0)),
                mark = datetime.datetime.now()

           
        d = datetime.datetime.now() - mark
        return count / ((d.seconds * 1000 + d.microseconds / 1000.0) / 1000.0)

    @unittest.skipIf(sys.modules.has_key('pywin'), 'not running perftest inside the IDE')
    def _____________test_perf(self):
        tests = (
                  {'type': 'volatile_map',      'hasKey': True},
                  {'type': 'persistent_map',    'hasKey': True},

                  {'type': 'volatile_list',      'hasKey': False},
                  {'type': 'persistent_list',    'hasKey': False},
                )

        count = 2 * 1000
        bytes = 30
        namePerfix = self.get_me_a_random_container_name() + '_'

        for test in tests:
            type = test['type']
            hasKey = test['hasKey']
            ds = self.hub.create(namePerfix + type, type)
            result = self.__speed_test(ds.set if hasKey else ds.push_back, count, bytes, hasKey)

            print '%s: %d msg/s' % (type, result)

class ContainerTests(InteliHubTestCase):
    def test_create_all_container_types(self):
        types = ('volatile_list', 'volatile_map', 'persistent_list', 'persistent_map')

        for t in types:
            self.hub.create(t + 'container', t)

        for t in types:
            self.hub.open(t + 'container', t)

    def test_dispatch_pending_max(self):
        '''
            Test if the 'max' parameter of dispatch_pending_events is being repected
        '''
        l = self.hub.create(self.get_me_a_random_container_name(), 'volatile_list')

        how_many = ListReceiveCounter(self)

        l.subscribe(how_many.on_event)

        for x in range(10):
            l.append(x)

        # just to guarantee we will receive all pending events from the server
        self.hub.ping()

        dispatched = l.dispatch_pending_events(1)
        self.assertEqual(dispatched, 1)
        self.assertEqual(how_many.receive_count, 1)

        dispatched = l.dispatch_pending_events(1)
        self.assertEqual(dispatched, 1)
        self.assertEqual(how_many.receive_count, 2)

        dispatched = l.dispatch_pending_events(3)
        self.assertEqual(dispatched, 3)
        self.assertEqual(how_many.receive_count, 5)

        dispatched = l.dispatch_pending_events()
        self.assertEqual(dispatched, 5)
        self.assertEqual(how_many.receive_count, 10)

    def test_volatile_list_get(self):
        container = self.hub.create(self.get_me_a_random_container_name(), 'volatile_list')
        self.__list_get_test(container)
        return container

    def test_persistent_list_get(self):
        container = self.hub.create(self.get_me_a_random_container_name(), 'persistent_list')
        self.__list_get_test(container)
        return container

    def __list_get_test(self, container):
        self.assertEqual(len(container), 0)

        container.clear()
        self.assertEqual(len(container), 0)

        def test_record_access(positive_index, negative_index):
            v = '%dv' % positive_index
            m = '%dm' % positive_index
            self.assertEqual(container.get(positive_index, withKeyAndMetadata=True), (positive_index, v, m))

            #
            # When accessing with negative index, the key returned by the server must be the positive one
            #
            self.assertEqual(container.get(negative_index, withKeyAndMetadata=True), (positive_index, v, m))
            self.assertEqual(container[positive_index], v)
            self.assertEqual(container[negative_index], v)

        test_count = 100

        for positive_index in range(test_count):
            v = '%dv' % positive_index
            m = '%dm' % positive_index
            container.push_back(v, m)

            test_record_access(positive_index, -1)

        for positive_index in range(test_count):
            test_record_access(positive_index, -test_count + positive_index)

           
        return container

    def test_queries(self):
        def do_all_queries(container):
            for x in container.query((len(container)/2)):
                pass
            for x in container.query(-(len(container)/2)):
                pass
                
            for x in container.keys():
                pass
            for x in container.values():
                pass
            for x in container.query():
                pass
            for x in container.query_with_key_and_metadata():
                pass
            

        do_all_queries(self.hub.create('pl', 'persistent_list'))
        do_all_queries(self.hub.create('pm', 'persistent_map'))

        container = self.hub.create('vl', 'volatile_list')
        container.extend([x for x in xrange(10)])
        do_all_queries(container)

        container = self.hub.create('vm', 'volatile_map')
        for x in range(10): container[str(x)] = x
        do_all_queries(container)


    def test_map_diff(self):
        #
        # TODO: verify results
        #
        container = self.hub.create(self.get_me_a_random_container_name(), 'volatile_map')
        diff = container.diff_start()

        for x in range(20) :
            container[str(x)] = x*x
        
        for x in range(10) :
            container[str(x)] = x*x

        container.clear()

       

    def test_list_diff(self):
        #
        # TODO: verify results
        #
        container = self.hub.create(self.get_me_a_random_container_name(), 'volatile_list')
        diff = container.diff_start()

        container.extend(range(100))
        container.extend(range(10))
        container.clear()


    def test_wait_and_pop(self):
        #
        # TODO: verify results
        #
        def f(key, value, metadata):
            pass
            
        container = self.hub.create(self.get_me_a_random_container_name(), 'volatile_list')
        container.wait_and_pop_next(f)
        container.append('abababu')
        container.wait_and_pop_next(f)
        container.append('xpto')


if __name__ == '__main__':
    unittest.main()