import socket
import functools
from cStringIO import StringIO
from datetime import datetime

separator = '^'

class TestSink:
    def __getattr__(self, name):
        return functools.partial(self.EventHandler, name)
    
    def EventHandler(self, eventName, key, value, metadata):
        print '%s - %s, %s, %s' % (eventName, key, value, metadata)

class RemoteContainer(object):
    def __init__(self, manager, handle, type, name):
        self.manager = manager
        self.handle = handle
        self.type = type
        self.name = name
        
        self.__dict__['Insert'] = functools.partial(self.send_data_command, 'insert')
        self.__dict__['Set'] = functools.partial(self.send_data_command, 'set')

        self.__dict__['SetProperty'] = functools.partial(self.send_data_command, 'set_property')
        self.__dict__['GetProperty'] = functools.partial(self.send_data_command, 'get_property')

    def __repr__(self):
        return '<tioclient.RemoteContainer name="%s", type="%s">' % (self.name, self.type)

    def __del__(self):
        self.Close()

    def __getitem__(self, key):
        if isinstance(key, slice):
            l = []
            start = key.start
            stop = key.stop
            if start is None:
                start = 0
            if stop is None:
                stop = len(self)
            for x in range(start, stop):
                l.append(self.get(x))
            return l                         
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
            
        return self.Set(key, value, metadata)

    def extend(self, iterable):
        for x in iterable:
            self.push_back(x)

    def get(self, key, withKeyAndMetadata=False):
        key, value, metadata = self.send_data_command('get', key, None, None)
        return value if not withKeyAndMetadata else (key, value, metadata)

    def delete(self, key, withKeyAndMetadata=False):
        key, value, metadata = self.send_data_command('delete', key, None, None)
        return value if not withKeyAndMetadata else (key, value, metadata)    

    def pop_back(self, withKeyAndMetadata=False):
        key, value, metadata = self.send_data_command('pop_back', None, None, None)
        return value if not withKeyAndMetadata else (key, value, metadata)

    def pop_front(self, withKeyAndMetadata=False):
        key, value, metadata = self.send_data_command('pop_front', None, None, None)
        return value if not withKeyAndMetadata else (key, value, metadata)    

    def push_back(self, value, metadata=None):
        return self.send_data_command('push_back', None, value, metadata)

    def push_front(self, value, metadata=None):
        return functools.partial(self.send_data_command, 'push_front', None, value, metadata)    
        
    def get_count(self):
        return int(self.manager.SendCommand('get_count', self.handle)['count'])

    def clear(self):
        return self.manager.SendCommand('clear', self.handle)
    
    def subscribe(self, sink, start = None):
        self.manager.Subscribe(self.handle, sink, start)

    def unsubscribe(self, sink):
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
  
'''
class Answer:
    def __init__(self):
        self.isAnswer = False
        self.type = None
        self.result = None
        self.parameterType = None
        self.parameter = None
'''

class TioServerConnection(object):
    def __init__(self, host = None, port = None):
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.receiveBuffer = ''
        self.pendingEvents = {}
        self.sinks = {}
        self.poppers = {}
        self.dontWaitForAnswers = False
        self.pendingAnswerCount = 0

        self.log_sends = False        

        if host:
            self.Connect(host, port)

    def __ReceiveLine(self):
        i = self.receiveBuffer.find('\r\n')
        while i == -1:
            self.receiveBuffer += self.s.recv(4096)
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
            elif e.name == 'wnp_next':
                f = self.poppers[int(handle)]['wnp_next'].pop()
            else:
                sink = self.sinks[handle]
                f = getattr(sink, e.name)

            if f:
                f(*e.data)

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

                if parameterType == 'handle' or parameterType == 'count' or parameterType == 'name':
                    return { parameterType : params[currentParam+1] }

                if parameterType == 'data':
                    return self.ReceiveDataAnswer(params, currentParam)

            elif answerType == 'event':
                class Event: pass
                
                event = Event()
                
                currentParam += 1
                event.handle = params[currentParam]

                currentParam += 1
                event.name = params[currentParam]

                if event.name != 'clear':                
                    event.data = self.ReceiveDataAnswer(params, currentParam)

                self.HandleEvent(event)

                if not wait_until_answer:
                    return

    def RunLoop(self):
        while 1:
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
                value = float(dataBuffer)
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
        elif type(data) is float:
            return (str(data), 'double')

        raise Exception('not supported data type')

    def Auth(self, token, password):
        self.SendCommand(' '.join( ('auth', token, 'clean', password) ))

    def SetPermission(self, objectType, objectName, command, allowOrDeny, user = ''):
        halfCommand = ' '.join( ('set_permission', objectType, objectName, command, allowOrDeny) )
        self.SendCommand(halfCommand + (' ' + user if user != '' else ''))

    def Subscribe(self, handle, sink, start = None):
        param = str(handle)
        if not start is None:
            param += ' ' + str(start)
        self.SendCommand('subscribe', param)
        self.sinks[handle] = sink

    def WaitAndPop(self, handle, wnp_type, sink, key = None):
        param = str(handle)

        if wnp_type == 'wnp_next':
            self.SendCommand('wnp_next', param)
            self.poppers.setdefault(int(handle), {}).setdefault('wnp_next', []).append(sink)
        elif wnp_type == 'wnp_key':
            if key is None or not type(key) is str:
                raise Exception('key is required and must be a string')
            self.SendDataCommand('wnp_key', handle, key, None, None)
            self.poppers.setdefault(int(handle), {}).setdefault('wnp_key', {}).setdefault(key, []).append(sink)
        else:
            raise Exception('invalid wait and pop type')
            

    def Unsubscribe(self, handle):
        self.SendCommand('unsubscribe', str(handle))
        del self.sinks[handle]
        
    def GetFieldSpec(self, fieldName, fieldDataAndType):
        if fieldDataAndType:
            return ' ' + fieldName + ' ' + fieldDataAndType[1] + ' ' + str(len(fieldDataAndType[0]))
        else:
            return ''

    def ReceivePendingAnswers(self):
        for x in xrange(self.pendingAnswerCount):
            self.pendingAnswerCount -= 1
            self.ReceiveAnswer()

    def SendCommand(self, command, parameter=None):
        buffer = command
        if not parameter is None and len(parameter) > 0:
            buffer += ' ' + parameter

        if buffer[-2:] != '\r\n':
            buffer += '\r\n'            

        self.s.sendall(buffer)

        if self.log_sends:
            print buffer,

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
            print buffer,

        return self.SendCommand(buffer)

    def Connect(self, host, port):
        self.s.connect((host, port))


    def Disconnect(self):
        self.s.close()
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        

    def __CreateOrOpenContainer(self, command, name, type):
        return RemoteContainer(self, self.SendCommand(command, name if type == '' else name + ' ' + type)['handle'], type, name)

    def CloseContainer(self, handle):
        self.SendCommand('close_container', handle)

    def CreateContainer(self, name, type):
        return self.__CreateOrOpenContainer('create_container', name, type)

    def OpenContainer(self, name, type = ''):
        return self.__CreateOrOpenContainer('open_container', name, type)

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

def DoTest():
    man = RemoteContainerManager()
    
    man.Connect('localhost', 6666)
    container = man.CreateContainer('name', 'volatile/list')

    container.Subscribe(TestSink())

    for x in range(100):
        print x
        container.PushBack(None, 10, 'metadata')
        container.Set(0, 'Rodrigo Strauss')
        container.Insert(0, 'test')
        container.PushFront(None, 12334567)

        key, value, metadata = container.Get(0)        

        man.DispatchAllEvents();    

    return

def SpeedTest(func, count, bytes, useKey = False):

    v = '*' * bytes
    
    mark = datetime.now()

    log_step = 2000

    for x in xrange(count):
        func(key=str(x) if useKey else None, value=v)
        if x % log_step == 0:
            d = datetime.now() - mark
            print "%d, %0.2f/s" % (x, log_step / ((d.seconds * 1000 + d.microseconds / 1000.0) / 1000.0))
            mark = datetime.now()

       
    d = datetime.now() - mark
    return count / ((d.seconds * 1000 + d.microseconds / 1000.0) / 1000.0)

def MasterSpeedTest():
    man = TioServerConnection()  
    man.Connect('localhost', 6666)

    tests = (
              {'type': 'volatile/map',      'hasKey': True},
              {'type': 'persistent/map',    'hasKey': True},

              {'type': 'volatile/list',      'hasKey': False},
              {'type': 'persistent/list',    'hasKey': False},
            )

    count = 50 * 1000
    bytes = 30
    namePerfix = datetime.now().strftime('%Y%m%d%H%M%S') + '_'

    for test in tests:
        type = test['type']
        hasKey = test['hasKey']
        ds = man.CreateContainer(namePerfix + type, type)
        print type
        result = SpeedTest(ds.Set if hasKey else ds.PushBack, count, bytes, hasKey)

        print '%s: %f msg/s' % (type, result)

def TestWaitAndPop():
    def f(key, value, metadata):
        print (key, value, metadata)
        
    man = TioServerConnection('localhost', 6666)
    container = man.CreateContainer('abc', 'volatile/vector')
    container.WaitAndPopNext(f)
    container.PushBack(key=None, value='abababu')
    container.WaitAndPopNext(f)
    container.WaitAndPopNext(f)
    container.PushBack(key=None, value='xpto')

    container = man.CreateContainer('xpto', 'volatile/vector')
    container.Set('key1', 'value1')
    container.Set('key2', 'value2')
    container.WaitAndPopKey('key1', f)
    container.WaitAndPopKey('key3', f)
    container.Set('key3', 'value3')
    container.Set('key4', 'value4')
        
    man.DispatchAllEvents()

def ParseUrl(url):
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

def OpenByUrl(url):
    address, port, container = ParseUrl(url)
    return TioServerConnection(address, port).OpenContainer(container)

def Connect(url):
    address, port, container = ParseUrl(url)
    if container:
        raise Exception('container specified, you must inform a url with just the server/port')
    
    return TioServerConnection(address, port)
        
if __name__ == '__main__':
    #DoTest()
    #BovTest()
    #BdbTest()
    ParseUrl('tio://127.0.0.1:6666/xpto/asas')
    MasterSpeedTest()
    #TestOrderManager()
    #TioConnectionsManager().ParseUrl('tio://localhost:6666')
    #TestWaitAndPop()

