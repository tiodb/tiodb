# we'll not use this because it works only on 2.6+
#from __future__ import print_function
from decimal import Decimal
import socket
import functools
from cStringIO import StringIO
from datetime import datetime
from decimal import Decimal
import weakref

TIO_DEFAULT_PORT = 2605

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

    def subscribe(self, sink, event_filter='*', start = None, end = None):
        self.manager.Subscribe(self.handle, sink, event_filter, start, end)

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

    def dispatch_pending_events(self, max=0xFFFFFFFF):
        return self.manager.DispatchPendingHandleEvents(self.handle, max)

class TioServerConnection(object):
    def __init__(self, host = None, port = None):
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.receiveBuffer = ''
        self.pendingEvents = {}
        self.sinks = {}
        self.group_sinks = {}
        self.poppers = {}
        self.wait_for_answers = True
        self.pending_answers_count = 0
        self.containers = {}
        self.stop = False

        self.host = None
        self.port = None

        self.log_sends = False

        self.running_queries = {}

        if host:
            self.Connect(host, port)

    def __del__(self):
       self.close()

    def close(self):
        self.s.close()

    def create(self, name, type=None):
        return self.__CreateOropen('create', name, type)

    def open(self, name, type = ''):
        return self.__CreateOropen('open', name, type)

    def group_add(self, group_name, container_name):
        return self.SendCommand('group_add', group_name, container_name)

    def group_subscribe(self, group_name, sink, start):
        self.group_sinks[group_name] = sink
        return self.SendCommand('group_subscribe', group_name, start)

        
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

    def DispatchPendingHandleEvents(self, handle, max=0xFFFFFFFF):
        handle = int(handle)
        events = self.pendingEvents.get(handle)

        if not events:
            return 0

        for index, e in enumerate(events):
            if index >= max:
                break

            if e.name == 'wnp_key':
                key = e.data[0]
                f = self.poppers[handle]['wnp_key'][key].pop()
                if f:
                    f(self.containers[e.handle], e.name, *e.data)
            elif e.name == 'wnp_next':
                f = self.poppers[handle]['wnp_next'].pop()
                if f:
                    f(self.containers[e.handle], e.name, *e.data)
            else:
                sinks = self.sinks[handle].get(e.name)
                # if there are no sink to this specific event, lets get the global (event='*')
                if sinks is None:
                    sinks = self.sinks[handle].get('*', [])
                for sink in sinks:
                    sink(self.containers[e.handle], e.name, *e.data)

        if max > len(events):
            index = len(events)
            self.pendingEvents[handle] = []
        else:
            del self.pendingEvents[handle][:index]

        return index

    def DispatchPendingEvents(self, max=0xFFFFFFFF):
        count = 0

        for handle in self.pendingEvents.keys():
            count += self.DispatchPendingHandleEvents(handle, max=max-count)
            if count >= max:
                break

        return count

    def DispatchPendingEventAndReceiveNext(self):
        self.DispatchPendingEvents()
        self.ReceiveAnswer(False)


    def RunLoop(self, timeout, max_events=1000000):
        self.s.settimeout(timeout)
        for x in xrange(max_events):
            try:
                self.DispatchPendingEvents()
                if self.stop:
                    break
                self.ReceiveAnswer(False)
            except socket.timeout:
                return

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

    def server_pause(self):
        return self.SendCommand('pause')

    def server_resume(self):
        return self.SendCommand('resume')

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

            elif answerType == 'group_container':
                group_name, container_name, container_type, container_handle = params[1:]
                container_handle = int(container_handle)
                self.containers[container_handle] = RemoteContainer(self, container_handle, container_type, container_name)
                sink = self.group_sinks[group_name]
                self.sinks.setdefault(container_handle, {}).setdefault('*', []).append(sink)
                
            elif answerType == 'event':
                class Event: pass

                event = Event()

                currentParam += 1
                event.handle = int(params[currentParam])

                currentParam += 1
                event.name = params[currentParam]

                self.HandleEvent(event)

                if event.name == 'clear' or event.name == 'snapshot_end':
                    event.data = None, None, None
                else:
                    event.data = self.ReceiveDataAnswer(params, currentParam)

                if not wait_until_answer:
                    return

    def Stop(self):
        self.stop = True

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

    def Subscribe(self, handle, sink, event_filter = '*', start = None, end = None):
        param = str(handle)

        if not start is None:
            param += ' ' + str(start)
            if not end is None:
                param += ' ' + str(end)

        #self.sinks[handle][filter](event_name, key, value, metadata)
        self.sinks.setdefault(int(handle), {}).setdefault(event_filter, []).append(sink)
        self.SendCommand('subscribe', param)

    def Unsubscribe(self, handle):
        self.SendCommand('unsubscribe', str(handle))
        del self.sinks[int(handle)]

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
        for x in xrange(self.pending_answers_count):
            self.pending_answers_count -= 1
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

        if not self.wait_for_answers:
            self.pending_answers_count += 1
            return

        try:
            return self.ReceiveAnswer()
        except Exception, ex:
            raise Exception('%s - "%s"' % (ex, buffer.strip('\r\n ')))

    def SendCommandAndForceAnswer(self, command, *args):
        if self.wait_for_answers:
            return self.SendCommand(command, *args)
        else:
            self.ReceivePendingAnswers()
            self.wait_for_answers = True
            ret = self.SendCommand(command, *args)
            self.wait_for_answers = False
            return ret

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
        self.host = host
        self.port = port
        self.s.connect((host, port))

    def Disconnect(self):
        self.s.close()
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def __CreateOropen(self, command, name, type):
        info = self.SendCommandAndForceAnswer(command, name if not type else name + ' ' + type)
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

def parse_url(url):
    if url[:6] == 'tio://':
        url = url[6:]

    slash_pos = url.find('/', 7)

    if slash_pos != -1:
        parts = (url[:slash_pos], url[slash_pos+1:])
    else:
        parts = (url,None)


    try:

        host_and_maybe_port = parts[0].split(':')

        if len(host_and_maybe_port) == 2:
            host, port = host_and_maybe_port
        else:
            host = host_and_maybe_port[0]
            port = TIO_DEFAULT_PORT

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


def main():
    tio = connect('tio://127.0.0.1')
    def sink(c, e, k, v, m): print c, e, k, v, m
    l = tio.create('xpto', 'volatile_list')
    l.subscribe(sink)

    return

if __name__ == '__main__':
    main()
