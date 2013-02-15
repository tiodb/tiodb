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
        if event_name != 'snapshot_end':            
            self.test_case.assertEqual(v, self.receive_count - 1)

        self.last_received_tuple = (k, v, m)
        self.receive_count += 1

class ListMirror(object):
    def __init__(self, test_case):
        self.l = []
        self.test_case = test_case

    def clear(self):
        self.l = []
        
    def on_event(self, container, event_name, k, v, m):
        if event_name == 'push_back':
            self.test_case.assertEqual(k, len(self.l))
            self.l.append(v)
        elif event_name == 'push_front':
            self.test_case.assertEqual(k, 0)
            self.l.insert(0, v)
        elif event_name == 'insert':
            self.l.insert(k, v)
        elif event_name == 'pop_front':
            del self.l[0]
        elif event_name == 'pop_back':
            del self.l[-1]
        elif event_name == 'delete':
            del self.l[k]
        elif event_name == 'clear':
            self.l = []
        elif event_name == 'set':
            self.l[k] = v

class InteliHubTestCase(unittest.TestCase):
    def setUp(self):
        self.hub = intelihubclient.connect('localhost')

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

    #@unittest.skipIf(sys.modules.has_key('pywin'), 'not running perftest inside the IDE')
    def test_perf(self):
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
        self.assertEqual(dispatched, 6)
        self.assertEqual(how_many.receive_count, 11)

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

    def test_events(self):
        container = self.hub.create(self.get_me_a_random_container_name(), 'volatile_list')
        mirror = ListMirror(self)
        container.subscribe(mirror.on_event)

        def check_mirror(expected_list_state):
            container.dispatch_pending_events()
            self.assertEqual(mirror.l, expected_list_state)

        container.append(0)
        check_mirror([0])

        container.append(1)
        check_mirror([0, 1])

        container.append(2)
        check_mirror([0, 1, 2])

        container.clear()
        check_mirror([])

        for x in range(100):
            container.append(x)
            check_mirror(range(x+1))

        container.clear()
        check_mirror([])

    def test_slice_subscribe(self):
        container = self.hub.create(self.get_me_a_random_container_name(), 'volatile_list')
        mirror = ListMirror(self)

        def check_mirror(expected_list_state):
            container.dispatch_pending_events()
            self.assertEqual(mirror.l, expected_list_state)

        #
        # test 1: slice [0:0]
        #
        container.subscribe(mirror.on_event, start = 0, end = 0)

        container.append(0)
        check_mirror([0])

        del container[0]
        check_mirror([])

        #clear

        container.append(0)
        check_mirror([0])        

        container.append(1)
        check_mirror([0])

        container.append(2)
        check_mirror([0])

        del container[0]
        check_mirror([1])

        del container[0]
        check_mirror([2])        

        del container[0]
        check_mirror([])

        #clear
        container.append(1)
        check_mirror([1])

        container.push_front(0)
        check_mirror([0])

        container.clear()
        container.unsubscribe()
        mirror.clear()
        
        #
        # test 1: slice [0:9]
        #
        container.subscribe(mirror.on_event, start = 0, end = 9)

        for x in range(10):
            container.append(x)
            check_mirror(range(x+1))

        # outside slice, should not reflect here
        for x in range(10,15):
            container.append(x)
            check_mirror([0,1,2,3,4,5,6,7,8,9])

        container.pop_front()
        check_mirror([1,2,3,4,5,6,7,8,9, 10])

        del container[0]
        check_mirror([2,3,4,5,6,7,8,9, 10, 11])

        del container[2]
        check_mirror([2,3,5,6,7,8,9, 10, 11, 12])

        container.pop_back()
        check_mirror([2,3,5,6,7,8,9, 10, 11, 12])

        container.insert(1, 'abc')
        check_mirror([2,'abc',3,5,6,7,8,9, 10, 11])

        del container[1]
        check_mirror([2,3,5,6,7,8,9, 10, 11, 12])        

        del container[9]
        check_mirror([2,3,5,6,7,8,9, 10, 11, 13])        

        del container[1]
        check_mirror([2,5,6,7,8,9, 10, 11, 13])

        del container[-2]
        check_mirror([2,5,6,7,8,9, 10, 13])

        del container[0]
        check_mirror([5,6,7,8,9, 10, 13])

        container.clear()
        container.unsubscribe()
        mirror.clear()
        
        #
        # test 1: slice [2:3]
        #
        container.subscribe(mirror.on_event, start = 2, end = 3)

        for x in range(6):
            container.append(x)
            if x < 2:
                check_mirror([])
            elif x == 2:
                check_mirror([2])
            else:
                check_mirror([2, 3])
                            
                
        container.push_front('asdasd')
        check_mirror([1,2])
        
        container.pop_front()
        check_mirror([2,3])

        container.append(6)
        check_mirror([2,3])

        container[2] = '2'
        check_mirror(['2',3])

        container.insert(3, '3')
        check_mirror(['2','3'])

        del container[3]
        check_mirror(['2',3])

        container.insert(2, 2)
        check_mirror([2,'2'])

        del container[3]
        check_mirror([2,3])        
        

        del container[-1]
        check_mirror([2,3])        
        
        del container[1]
        check_mirror([3, 4])

        del container[2]
        check_mirror([4, 5])

        del container[-1]
        check_mirror([4])

        container.push_front(0)
        check_mirror([2, 4])

        container.clear()
        container.unsubscribe()
        mirror.clear()        

        #
        # test 1: slice [-2:-1]
        #
        container.subscribe(mirror.on_event, start = -2, end = -1)

        container.append(0)
        check_mirror([0])

        container.append(1)
        check_mirror([0, 1])

        container.append(2)
        check_mirror([1,2])

        container.append(3)
        check_mirror([2, 3])

    def test_group_subscribe(self):
        container_count = 1000
        start_item_count = 50
        added_item_count = 50

        print 'creating containers...'
        containers = [self.hub.create(self.get_me_a_random_container_name()) for x in range(container_count)]

        def sink(*args):
            pass#print args

        print 'adding items...'
        for index, container in enumerate(containers):
            for x in range(start_item_count):
                container.push_back(str(index))

        print 'adding to group containers...'
        for container in containers:
            self.hub.group_add('test_group', container.name)

        print 'group subscribe...'
        self.hub.group_subscribe('test_group', sink, 0)

        self.hub.ping()
        self.hub.DispatchPendingEvents()

        print 'adding items to container after subscription'
        for index, container in enumerate(containers):
            for x in range(added_item_count):
                container.push_back(str(index) * 2)

        self.hub.DispatchPendingEvents()

        
if __name__ == '__main__':
    unittest.main()