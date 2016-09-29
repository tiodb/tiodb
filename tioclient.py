# we'll not use this because it works only on 2.6+
#from __future__ import print_function
from decimal import Decimal
import socket
import functools
from cStringIO import StringIO
from datetime import datetime
from decimal import Decimal
import weakref

separator = '^'

__connections = {}

def decode(value):
    '''
        transforms 'X10003C0002I000aS0000X 12 abcdefghij' into [12, 'abcde']

        X1 = protocol version
        0003C = field count
        0002I = first is an integer, codified into 2 bytes
        000aS = second field is an string with 0xA bytes
        0000X = NULL field

        there's always an space between the values, and IT'S NOT taken into account
        when calculating the field size
        
    '''
    if value[:2] != 'X1':
        raise Exception('not a valid message')
    
    header_size = 2
    field_count = int(value[header_size:header_size+4], 16) # expect hex
    current_data_offset = header_size + 1 + (field_count + 1) * 5
    ret = []

    for x in range(field_count):
        size_start = header_size + 5 * (x + 1)
        field_size = int(value[size_start:size_start+4], 16)
        data_type = value[size_start + 4: size_start + 5]

        field_value = value[current_data_offset:current_data_offset+field_size]

        if data_type == 'D':
            field_value = Decimal(field_value)
        elif data_type == 'I':
            field_value = int(field_value)
        elif data_type == 'X':
            field_value = None

        current_data_offset += field_size + 1

        ret.append(field_value)

    return ret

type_map = {}
type_map[int] = 'I'
type_map[Decimal] = 'D'
type_map[str] = 'S'
type_map[unicode] = 'S'

def encode(values):
    header_io = StringIO()
    values_io = StringIO()

    header_io.write('X1')
    header_io.write('%04XC' % len(values))

    for v in values:
        strv = str(v)
        header_io.write('%04X%s' % (len(strv), type_map[type(v)]))
        values_io.write(strv) ; values_io.write(' ')

    header_io.write(' ')
    header_io.write(values_io.getvalue())

    encoded_value = header_io.getvalue()
    return encoded_value

#
# this class is meant to be inherited by container classes,
# just to make them more pythonic
#
class ContainerPythonizer(object):
    def __del__(self):
        self.Close()

    def __getitem__(self, key):
        if isinstance(key, slice):
            return self.query(key.start, key.stop)
        else:
            return self.get(key)

    def __delitem__(self, key):
        return self.delete(key)    

    def __len__(self):
        return self.get_count()    

    def __setitem__(self, key, valueOrValueAndMetadata):
        if isinstance(valueOrValueAndMetadata, tuple):
            value, metadata = valueOrValueAndMetadata
        else:
            value, metadata = valueOrValueAndMetadata, None
            
        return self.set(key, value, metadata)

    def append(self, value, metadata=None):
        return self.push_back(value, metadata)

    def extend(self, iterable):
        for x in iterable:
            self.push_back(x)
            
    def values(self):
        return self.query()    

    def keys(self):
        return [x[0] for x in self.query_with_key_and_metadata()]

    def __fluffler():
        container.__dict__['__del__'] = __del__
        container.__dict__['__getitem__'] = __getitem__
        container.__dict__['__delitem__'] = __delitem__
        container.__dict__['__len__'] = __len__
        container.__dict__['__setitem__'] = __setitem__
        container.__dict__['append'] = append
        container.__dict__['extend'] = extend
        container.__dict__['values'] = values
        container.__dict__['keys'] = keys


class RemoteContainer(ContainerPythonizer):
    def __init__(self, manager, handle, type, name):
        self.manager = manager
        self.handle = handle
        self.type = type
        self.name = name
        
    def __repr__(self):
        return '<tioclient.RemoteContainer name="%s", type="%s">' % (self.name, self.type)
        
    def propget(self, key, withKeyAndMetadata=False):
        key, value, metadata = self.send_data_command('get_property', key, None, None)
        return value if not withKeyAndMetadata else (key, value, metadata)    
        
    def propset(self, key, value, metadata=None):
        return self.send_data_command('set_property', key, value, metadata)

    def get(self, key, withKeyAndMetadata=False):
        key, value, metadata = self.send_data_command('get', key, None, None)
        return value if not withKeyAndMetadata else (key, value, metadata)

    def delete(self, key):
        self.send_data_command('delete', key, None, None)

    def pop_back(self, withKeyAndMetadata=False):
        key, value, metadata = self.send_data_command('pop_back', None, None, None)
        return value if not withKeyAndMetadata else (key, value, metadata)

    def pop_front(self, withKeyAndMetadata=False):
        key, value, metadata = self.send_data_command('pop_front', None, None, None)
        return value if not withKeyAndMetadata else (key, value, metadata)        

    def insert(self, key, value, metadata=None):
        return self.send_data_command('insert', key, value, metadata)

    def set(self, key, value, metadata=None):
        return self.send_data_command('set', key, value, metadata)    
    
    def push_back(self, value, metadata=None):
        return self.send_data_command('push_back', None, value, metadata)

    def push_front(self, value, metadata=None):
        return self.send_data_command('push_front', None, value, metadata)
        
    def get_count(self):
        return int(self.manager.SendCommand('get_count', self.handle)['count'])

    def clear(self):
        return self.manager.SendCommand('clear', self.handle)
    
    def subscribe(self, sink, event_filter='*', start = None):
        self.manager.Subscribe(self.handle, sink, event_filter, start)

    def unsubscribe(self):
        self.manager.Unsubscribe(self.handle)

    def close(self):
        self.manager.CloseContainer(self.handle)
    
    def send_data_command(self, command, key = None, value = None, metadata = None):
        return self.manager.SendDataCommand(command, self.handle, key, value, metadata)

    def set_permission(self, command, allowOrDeny, user = ''):
        return self.manager.SetPermission(self.type, self.name, command, allowOrDeny, user)

    def wait_and_pop_key(self, key, sink):
        return self.manager.WaitAndPop(self.handle, 'wnp_key', sink, key)

    def wait_and_pop_next(self, sink):
        return self.manager.WaitAndPop(self.handle, 'wnp_next', sink)

    def start_recording(self, destination_container):
        return self.manager.SendCommand('start_recording', self.handle, destination_container.handle)

    def query(self, startOffset=None, endOffset=None):
        # will return only the values
        return [x[1] for x in self.query_with_key_and_metadata(startOffset, endOffset)]

    def query_with_key_and_metadata(self, startOffset=None, endOffset=None):
        return self.manager.Query(self.handle, startOffset, endOffset)

    def diff_start(self):
        result = self.manager.DiffStart(self.handle)
        return result['diff_handle']

    def diff_query(self, diff_handle):
        return self.manager.Diff(diff_handle)

class TioServerConnection(object):
    def __init__(self, host = None, port = None):
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.receiveBuffer = ''
        self.pendingEvents = {}
        self.sinks = {}
        self.poppers = {}
        self.dontWaitForAnswers = False
        self.pendingAnswerCount = 0
        self.containers = {}
        self.stop = False

        self.log_sends = False

        self.running_queries = {}

        if host:
            self.Connect(host, port)

    def __del__(self):
       self.close() 

    def close(self):
        self.s.close()
        
    def create(self, name, type):
        return self.__CreateOropen('create', name, type)

    def open(self, name, type = ''):
        return self.__CreateOropen('open', name, type)

    def __ReceiveLine(self):
        i = self.receiveBuffer.find('\r\n')
        while i == -1:
            self.receiveBuffer += self.s.recv(4096)
            if not self.receiveBuffer:
                raise Exception('error reading from connection socket')
            
            i = self.receiveBuffer.find('\r\n')

        ret = self.receiveBuffer[:i]
        self.receiveBuffer = self.receiveBuffer[i+2:]

        return ret

    def DispatchEvents(self, handle):
        events = self.pendingEvents.get(handle)

        if not events:
            return
        
        for e in events:
            if e.name == 'wnp_key':
                key = e.data[0]
                f = self.poppers[int(handle)]['wnp_key'][key].pop()
                if f:
                    f(self.containers[e.handle], e.name, *e.data)
            elif e.name == 'wnp_next':
                f = self.poppers[int(handle)]['wnp_next'].pop()
                if f:
                    f(self.containers[e.handle], e.name, *e.data)
            else:
                handle = int(handle)
                sinks = self.sinks[handle].get(e.name)
                if sinks is None:
                    sinks = self.sinks[handle].get('*', [])
                for sink in sinks:
                    sink(self.containers[e.handle], e.name, *e.data)
           

        self.pendingEvents[handle] = []            

    def DispatchAllEvents(self):
        for handle in self.pendingEvents.keys():
            self.DispatchEvents(handle)

    def __ReceiveData(self, size):
        while len(self.receiveBuffer) < size:
            self.receiveBuffer += self.s.recv(4096)
            
        ret = self.receiveBuffer[:size]
        self.receiveBuffer = self.receiveBuffer[size:]

        return ret

    def RegisterQuery(self, query_id):
        self.running_queries[query_id] = []

    def AddToQuery(self, query_id, data):
        self.running_queries[query_id].append(data)

    def FinishQuery(self, query_id):
        query = self.running_queries[query_id]
        del self.running_queries[query_id]
        return query

    def ping(self):
        return self.SendCommand('ping')
                
    def ReceiveAnswer(self, wait_until_answer = True):
        while 1:        
            line = self.__ReceiveLine()
            params = line.split(' ')
            currentParam = 0

            answerType = params[currentParam]

            if answerType == 'answer':
                currentParam += 1
                answerResult = params[currentParam]

                if answerResult != 'ok':
                    raise Exception(line)

                currentParam += 1

                # just an ok, no data, no handle. just a happy end
                if currentParam + 1 > len(params):
                    return
                    
                parameterType = params[currentParam]

                #
                # ending space...
                #
                if parameterType == '':
                    return

                if parameterType == 'pong':
                    return ' '.join(params[currentParam:])

                if parameterType == 'handle':
                    return { 'handle' : params[currentParam+1], 'type':  params[currentParam+2]}

                if parameterType == 'diff_map' or parameterType == 'diff_list':
                    return { 'diff_type' : parameterType, 'diff_handle':  params[currentParam+1] }
                
                if parameterType == 'count' or parameterType == 'name':
                    return { parameterType : params[currentParam+1] }

                if parameterType == 'data':
                    return self.ReceiveDataAnswer(params, currentParam)

                if parameterType == 'query':
                    query_id = params[currentParam+1]
                    self.RegisterQuery(query_id)
                    continue

                raise Exception('invalid parameter type: ' + parameterType) 

            elif answerType == 'diff_list' or answerType == 'diff_map':
                diff_handle = params[1]
                return (answerType, diff_handle)
            
            elif answerType == 'query':
                query_id = params[1]
                what = params[2]

                # query [id] (item|end) [data]
                if what == 'item':
                    self.AddToQuery(query_id, self.ReceiveDataAnswer(params, 2))
                elif what == 'end':
                    return self.FinishQuery(query_id)
                
            elif answerType == 'event':
                class Event: pass
                
                event = Event()
                
                currentParam += 1
                event.handle = int(params[currentParam])

                currentParam += 1
                event.name = params[currentParam]

                if event.name != 'clear':                
                    event.data = self.ReceiveDataAnswer(params, currentParam)

                self.HandleEvent(event)

                if not wait_until_answer:
                    return

    def RunLoop(self):
        while 1:
            self.Dispatch()
            if self.stop:
                self.DispatchAllEvents()
                return

    def Stop(self):
        self.stop = True

    def Dispatch(self):
        self.DispatchAllEvents()
        self.ReceiveAnswer(False)
                    
    def HandleEvent(self, event):
        self.pendingEvents.setdefault(event.handle, []).append(event)
            
    def ReceiveDataAnswer(self, params, currentParam):
        fields = {}
        while currentParam + 1 < len(params):
            currentParam += 1
            name = params[currentParam]

            currentParam += 1
            type = params[currentParam]

            currentParam += 1
            size = int(params[currentParam])

            dataBuffer = self.__ReceiveData(size + 2)[:-2] # discard \r\n

            if type == 'int':
                value = int(dataBuffer)
            elif type == 'double':
                value = Decimal(dataBuffer)
            elif type == 'string':
                value = dataBuffer
            else:
                raise Exception('unsupported data type: %s' % type)
                
            fields[name] = value
            
        return (fields.get('key'), fields.get('value'), fields.get('metadata'))
        
    def SerializeData(self, data):
        if data is None:
            return None
        
        if type(data) is str:
            return (data, 'string')
        elif type(data) is int or type(data) is long:
            return (str(data), 'int')
        elif type(data) in (Decimal, float):
            return (str(data), 'double')

        raise Exception('not supported data type')

    def Auth(self, token, password):
        self.SendCommand(' '.join( ('auth', token, 'clean', password) ))

    def DiffStart(self, handle):
        diff_result = self.SendCommand('diff_start ' + str(handle))
        return diff_result

    def Diff(self, diff_handle):
        return self.SendCommand('diff ' + str(diff_handle))

    def SetPermission(self, objectType, objectName, command, allowOrDeny, user = ''):
        halfCommand = ' '.join( ('set_permission', objectType, objectName, command, allowOrDeny) )
        self.SendCommand(halfCommand + (' ' + user if user != '' else ''))

    def Subscribe(self, handle, sink, filter = '*', start = None):
        param = str(handle)
        if not start is None:
            param += ' ' + str(start)
        
        #self.sinks[handle][filter](event_name, key, value, metadata)
        self.sinks.setdefault(int(handle), {}).setdefault(filter, []).append(sink)
        self.SendCommand('subscribe', param)

    def Unsubscribe(self, handle):
        self.SendCommand('unsubscribe', str(handle))
        del self.sinks[handle]        

    def WaitAndPop(self, handle, wnp_type, sink, key = None):
        param = str(handle)

        if wnp_type == 'wnp_next':
            self.poppers.setdefault(int(handle), {}).setdefault('wnp_next', []).append(sink)
            self.SendCommand('wnp_next', param)
        elif wnp_type == 'wnp_key':
            if key is None or not type(key) is str:
                raise Exception('key is required and must be a string')
            self.SendDataCommand('wnp_key', handle, key, None, None)
            self.poppers.setdefault(int(handle), {}).setdefault('wnp_key', {}).setdefault(key, []).append(sink)
        else:
            raise Exception('invalid wait and pop type')
                    
    def GetFieldSpec(self, fieldName, fieldDataAndType):
        if fieldDataAndType:
            return ' ' + fieldName + ' ' + fieldDataAndType[1] + ' ' + str(len(fieldDataAndType[0]))
        else:
            return ''

    def ReceivePendingAnswers(self):
        for x in xrange(self.pendingAnswerCount):
            self.pendingAnswerCount -= 1
            self.ReceiveAnswer()

    def SendCommand(self, command, *args):
        buffer = command
        if len(args):
            buffer += ' '
            buffer += ' '.join([str(x) for x in args])
        
        if buffer[-2:] != '\r\n':
            buffer += '\r\n'            

        self.s.sendall(buffer)

        if self.log_sends:
            print buffer

        if self.dontWaitForAnswers:
            self.pendingAnswerCount += 1
            return
        
        try:
            return self.ReceiveAnswer()
        except Exception, ex:
            raise Exception('%s - "%s"' % (ex, buffer.strip('\r\n ')))
    
    def SendDataCommand(self, command, parameter, key, value, metadata):
        buffer = command
        if not parameter is None and len(parameter) > 0:
            buffer += ' ' + parameter

        key = self.SerializeData(key)
        value = self.SerializeData(value)
        metadata = self.SerializeData(metadata)

        buffer += self.GetFieldSpec('key', key)
        buffer += self.GetFieldSpec('value', value)
        buffer += self.GetFieldSpec('metadata', metadata)

        buffer += '\r\n'

        if key:
            buffer += key[0] + '\r\n'
        if value:
            buffer += value[0] + '\r\n'
        if metadata:
            buffer += metadata[0] + '\r\n'

        if self.log_sends:
            print buffer

        return self.SendCommand(buffer)

    def Connect(self, host, port):
        self.s.connect((host, port))

    def Disconnect(self):
        self.s.close()
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        
    def __CreateOropen(self, command, name, type):
        info = self.SendCommand(command, name if not type else name + ' ' + type)
        handle = info['handle']
        type = info['type']
        container = RemoteContainer(self, handle, type, name)
        self.containers[int(handle)] = container
        return container

    def CloseContainer(self, handle):
        self.SendCommand('close', handle)

    def Query(self, handle, startOffset=None, endOffset=None):
        l = []
        l.append('query')
        l.append(handle)
        if not startOffset is None: l.append(str(startOffset))
        if not endOffset is None: l.append(str(endOffset))

        return self.SendCommand(' '.join(l))
        
class FieldParser:
    def __init__(self):
        pass

    def SetSchema(self, schema):
        self.keys = schema.split(separator)
        self.values = {}

    def Load(self, value):
        values = value.split(separator)

        if len(values) != len(self.keys):
            raise Exception('invalid message, field count doesn\'t match')

        self.values = dict(zip(self.keys, values))
        self.rawValues = values

    def Serialize(self):
        lst = []
        for k in self.keys:
            lst.append(str(self.values.get(k, '')))

        return separator.join(lst)

    def __getitem__(self, key):
        return self.values[key]

    def __setitem__(self, key, value):
        if not key in self.keys:
            raise Exception('invalid key')

        self.values[key] = value

def SpeedTest(func, count, bytes, useKey = False):

    v = '*' * bytes
    
    mark = datetime.now()

    log_step = 2000

    for x in xrange(count):
        func(key=str(x) if useKey else None, value=v)
        if x % log_step == 0 and x != 0:
            d = datetime.now() - mark
            print "%d, %0.2f/s" % (x, log_step / ((d.seconds * 1000 + d.microseconds / 1000.0) / 1000.0))
            mark = datetime.now()

       
    d = datetime.now() - mark
    return count / ((d.seconds * 1000 + d.microseconds / 1000.0) / 1000.0)

def MasterSpeedTest():
    man = connect('tio://127.0.0.1:6666')

    tests = (
              {'type': 'volatile_map',      'hasKey': True},
              {'type': 'persistent_map',    'hasKey': True},

              {'type': 'volatile_list',      'hasKey': False},
              {'type': 'persistent_list',    'hasKey': False},
            )

    count = 20 * 1000
    bytes = 30
    namePerfix = datetime.now().strftime('%Y%m%d%H%M%S') + '_'

    for test in tests:
        type = test['type']
        hasKey = test['hasKey']
        ds = man.create(namePerfix + type, type)
        print type
        result = SpeedTest(ds.set if hasKey else ds.push_back, count, bytes, hasKey)

        print '%s: %f msg/s' % (type, result)

def TestWaitAndPop():
    def f(key, value, metadata):
        print (key, value, metadata)
        
    man = TioServerConnection('localhost', 6666)
    container = man.create('abc', 'volatile_vector')
    container.WaitAndPopNext(f)
    container.PushBack(key=None, value='abababu')
    container.WaitAndPopNext(f)
    container.WaitAndPopNext(f)
    container.PushBack(key=None, value='xpto')

    container = man.create('xpto', 'volatile_vector')
    container.Set('key1', 'value1')
    container.Set('key2', 'value2')
    container.WaitAndPopKey('key1', f)
    container.WaitAndPopKey('key3', f)
    container.Set('key3', 'value3')
    container.Set('key4', 'value4')
        
    man.DispatchAllEvents()

def parse_url(url):
    if url[:6] != 'tio://':
        raise Exception ('protocol not supported')

    slash_pos = url.find('/', 7)

    if slash_pos != -1:
        parts = (url[6:slash_pos], url[slash_pos+1:])
    else:
        parts = (url[6:],None)
    

    try:
        host, port = parts[0].split(':')

        port = int(port)        

        # data container name is optional
        return (host, port, parts[1]) if len(parts) == 2 else (host, port, None)
    except Exception, ex:
        print ex
        raise Exception ('Not supported. Format must be "tio://host:port/[container_name]"')

def open_by_url(url, create_container_type=None):
    address, port, container = parse_url(url)

    if not container:
        raise Exception('url "%s" doesn\'t have a container specification' % url)
    
    server = TioServerConnection(address, port)
    if create_container_type:
        return server.create(container, create_container_type)
    else:
        return server.open(container)

def connect(url):
    address, port, container = parse_url(url)
    if container:
        raise Exception('container specified, you must inform a url with just the server/port')
    
    return TioServerConnection(address, port)

def TestQuery():
    tio = connect('tio://127.0.0.1:6666')

    def do_all_queries(container):
        print container
        try:
            for x in container.query((len(container)/2)):
                print x
            for x in container.query(-(len(container)/2)):
                print x
        except:
            pass
            
        for x in container.keys():
            print x
        for x in container.values():
            print x
        for x in container.query():
            print x
        for x in container.query_with_key_and_metadata():
            print x
        

    do_all_queries(tio.create('pl', 'persistent_list'))
    do_all_queries(tio.create('pm', 'persistent_map'))

    container = tio.create('vl', 'volatile_list')
    container.extend([x for x in xrange(10)])
    do_all_queries(container)

    container = tio.create('vm', 'volatile_map')
    for x in range(10): container[str(x)] = x
    do_all_queries(container)

def DiffTest():
    def DiffTest_Map():
        tio = connect('tio://127.0.0.1:6666')
        vm = tio.create('vm', 'volatile_map')
        diff = vm.diff_start()

        for x in range(20) : vm[str(x)] = x*x

        print vm.diff_query(diff)

        for x in range(10) : vm[str(x)] = x*x

        vm.clear()

        print vm.diff_query(diff)

    def DiffTest_List():
        tio = connect('tio://127.0.0.1:6666')
        vl = tio.create('vl', 'volatile_list')
        diff = vl.diff_start()

        vl.extend(range(100))

        print vl.diff_query(diff)

        vl.extend(range(10))

        vl.clear()

        print vl.diff_query(diff)

    DiffTest_List()
    DiffTest_Map()

def DoTest():
    server = connect('tio://127.0.0.1:6666')
    container = server.create('test123', 'volatile_list')

    container.clear()    

    def show(*args):
        print args
    
    container.subscribe(show)

    for x in range(1):
        print x
        container.push_back(10, 'metadata')
        container[0] = 'Rodrigo Strauss'
        container.insert(0, 'test')
        container.push_front(value=12334567)
        assert len(container) == 3

        key, value, metadata = container.get(0, withKeyAndMetadata=True)        

    server.DispatchAllEvents();

    return

       
if __name__ == '__main__':
    #Connect('tio://127.0.0.1:6666').ping()
    #DiffTest()
    #TestQuery()
    DoTest()
    #BdbTest()
    #parse_url('tio://127.0.0.1:6666/xpto/asas')
    #MasterSpeedTest()
    #TestOrderManager()
    #TioConnectionsManager().parse_url('tio://localhost:6666')
    #TestWaitAndPop()
