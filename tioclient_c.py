from ctypes import *
from decimal import Decimal
from cStringIO import StringIO

"""
struct TIO_DATA
{    
    unsigned int data_type;
    int int_;
    char* string_;
    double double_;
};
"""

TIO_DATA_TYPE_NONE   = 1
TIO_DATA_TYPE_STRING = 2
TIO_DATA_TYPE_INT    = 3
TIO_DATA_TYPE_DOUBLE = 4

class C_TIO_DATA(Structure):
    _fields_ = [
        ("data_type", c_uint),
        ("int_", c_int),
        ("string_", c_char_p),
        ("double_", c_double)]

class TioClientDll:
    def __init__(self):
        self.dll = cdll.LoadLibrary(r'D:\Strauss\code\tio\Release\tioclient.dll')
        
        # tio_initialize()
        self.tio_initialize = self.dll.tio_initialize
        self.tio_initialize.argtypes = []

        # tio_connect(const char* host, short port, struct TIO_CONNECTION** connection);
        self.tio_connect = self.dll.tio_connect
        self.tio_connect.argtypes = [c_char_p, c_short, POINTER(c_void_p)]

        # tio_disconnect()
        self.tio_disconnect = self.dll.tio_disconnect
        self.tio_disconnect.argtypes = []        

        # tio_create(struct TIO_CONNECTION* connection, const char* name, const char* type, struct TIO_CONTAINER** container);
        self.tio_create = self.dll.tio_create
        self.tio_create.argtypes = [c_void_p, c_char_p, c_char_p, POINTER(c_void_p)]
        
        # tio_open(struct TIO_CONNECTION* connection, const char* name, const char* type, struct TIO_CONTAINER** container);
        self.tio_open = self.dll.tio_open
        self.tio_open.argtypes = [c_void_p, c_char_p, c_char_p, POINTER(c_void_p)]
        
        # tio_close(struct TIO_CONTAINER* container);
        self.tio_close = self.dll.tio_close
        self.tio_close.argtypes = [c_void_p]
        
        # tio_dispatch_pending_events(struct TIO_CONNECTION* connection, unsigned int max_events);
        self.tio_dispatch_pending_events = self.dll.tio_dispatch_pending_events
        self.tio_dispatch_pending_events.argtypes = [c_void_p, c_int]
        
        # tio_ping(struct TIO_CONNECTION* connection, char* payload);
        self.tio_ping = self.dll.tio_ping
        self.tio_ping.argtypes = [c_void_p, c_char_p]
        
        # int tio_container_get(struct TIO_CONTAINER* container, const struct C_TIO_DATA* search_key, struct C_TIO_DATA* key, struct C_TIO_DATA* value, struct C_TIO_DATA* metadata);
        self.tio_container_get = self.dll.tio_container_get
        self.tio_container_get.argtypes = [c_void_p, POINTER(C_TIO_DATA), POINTER(C_TIO_DATA), POINTER(C_TIO_DATA), POINTER(C_TIO_DATA)]
        
        # int tio_container_get_count(struct TIO_CONTAINER* container, int* count);
        self.tio_container_get_count = self.dll.tio_container_get_count
        self.tio_container_get_count.argtypes = [c_void_p, POINTER(c_int)]
        
        container_funcs = [\
            'tio_container_clear',
            'tio_container_unsubscribe',
            ]
        
        container_key_funcs = [ \
            'tio_container_delete',
            ]
            
        container_key_value_funcs = [\
            'tio_container_propset',
            'tio_container_propget',
            ]
        
        container_key_value_metadata_funcs = [\
            'tio_container_push_back', 
            'tio_container_push_front',
            'tio_container_pop_back',
            'tio_container_pop_front',
            'tio_container_set',
            'tio_container_insert',
            ]
        
        for func_name in container_funcs:
            func = getattr(self.dll, func_name)
            func.argtypes = [c_void_p,]
            self.__dict__[func_name] = func
            
        for func_name in container_key_funcs:
            func = getattr(self.dll, func_name)
            func.argtypes = [c_void_p, POINTER(C_TIO_DATA)]
            self.__dict__[func_name] = func
            
        for func_name in container_key_value_funcs:
            func = getattr(self.dll, func_name)
            func.argtypes = [c_void_p, POINTER(C_TIO_DATA), POINTER(C_TIO_DATA)]
            self.__dict__[func_name] = func
            
        for func_name in container_key_value_metadata_funcs:
            func = getattr(self.dll, func_name)
            func.argtypes = [c_void_p, POINTER(C_TIO_DATA), POINTER(C_TIO_DATA), POINTER(C_TIO_DATA)]
            self.__dict__[func_name] = func

        #
        # On callback type definition, the first type is the function return type
        #
        
        #typedef void (*event_callback_t)(void* /*cookie*/, unsigned int /*handle*/, unsigned int /*event_code*/, const struct TIO_DATA*, const struct TIO_DATA*, const struct TIO_DATA*);
        self.event_callback_t = CFUNCTYPE(None, c_void_p, c_uint, c_uint, POINTER(C_TIO_DATA), POINTER(C_TIO_DATA), POINTER(C_TIO_DATA))
        
        # typedef void (*__query_callback_t)(void* /*cookie*/, unsigned int /*queryid*/, const struct C_TIO_DATA*, const struct C_TIO_DATA*, const struct C_TIO_DATA*);
        self.query_callback_t = CFUNCTYPE(None, c_void_p, c_uint, POINTER(C_TIO_DATA), POINTER(C_TIO_DATA), POINTER(C_TIO_DATA))

        
        # int tio_container_query(struct TIO_CONTAINER* container, int start, int end, __query_callback_t __query_callback, void* cookie);
        self.tio_container_query = self.dll.tio_container_query
        self.tio_container_query.argtypes = [c_void_p, c_int, c_int, self.query_callback_t, c_void_p]
        
        
        # int tio_container_subscribe(struct TIO_CONTAINER* container, struct C_TIO_DATA* start, event_callback_t event_callback, void* cookie);
        self.tio_container_subscribe = self.dll.tio_container_subscribe
        self.tio_container_subscribe.argtypes = [c_void_p, POINTER(C_TIO_DATA), self.event_callback_t, c_void_p]
        
        
        # tio_dispatch_pending_events(struct TIO_CONNECTION* connection, unsigned int max_events);
        self.tio_dispatch_pending_events = self.dll.tio_dispatch_pending_events
        self.tio_dispatch_pending_events.argtypes = [c_void_p, c_uint]
        

        # tiodata_init(struct C_TIO_DATA* tiodata);
        self.tiodata_init = self.dll.tiodata_init
        self.tiodata_init.argtypes = [POINTER(C_TIO_DATA),]
        
        # int tiodata_get_type(struct C_TIO_DATA* tiodata);
        self.tiodata_get_type = self.dll.tiodata_get_type
        self.tiodata_get_type.argtypes = [POINTER(C_TIO_DATA),]
        
        # tiodata_set_as_none(struct C_TIO_DATA* tiodata);
        self.tiodata_set_as_none = self.dll.tiodata_set_as_none
        self.tiodata_set_as_none.argtypes = [POINTER(C_TIO_DATA),]
        
        # tiodata_set_string_get_buffer(struct C_TIO_DATA* tiodata, unsigned int min_size);
        self.tiodata_set_string_get_buffer = self.dll.tiodata_set_string_get_buffer
        self.tiodata_set_string_get_buffer.argtypes = [POINTER(C_TIO_DATA), c_uint]
        
        # tiodata_set_string(struct C_TIO_DATA* tiodata, const char* value);
        self.tiodata_set_string = self.dll.tiodata_set_string
        self.tiodata_set_string.argtypes = [POINTER(C_TIO_DATA), c_char_p]
        
        # tiodata_set_int(struct C_TIO_DATA* tiodata, int value);
        self.tiodata_set_int = self.dll.tiodata_set_int
        self.tiodata_set_int.argtypes = [POINTER(C_TIO_DATA), c_int]
        
        # tiodata_set_double(struct C_TIO_DATA* tiodata, double value);
        self.tiodata_set_double = self.dll.tiodata_set_double
        self.tiodata_set_double.argtypes = [POINTER(C_TIO_DATA), c_double]

        self.TIO_COMMAND_PING = 0x10
        self.TIO_COMMAND_OPEN = 0x11
        self.TIO_COMMAND_CREATE = 0x12
        self.TIO_COMMAND_CLOSE = 0x13
        self.TIO_COMMAND_SET = 0x14
        self.TIO_COMMAND_INSERT = 0x15
        self.TIO_COMMAND_DELETE = 0x16
        self.TIO_COMMAND_PUSH_BACK = 0x17
        self.TIO_COMMAND_PUSH_FRONT = 0x18
        self.TIO_COMMAND_POP_BACK = 0x19
        self.TIO_COMMAND_POP_FRONT = 0x1A
        self.TIO_COMMAND_CLEAR = 0x1B
        self.TIO_COMMAND_COUNT = 0x1C
        self.TIO_COMMAND_GET = 0x1D
        self.TIO_COMMAND_SUBSCRIBE = 0x1E
        self.TIO_COMMAND_UNSUBSCRIBE = 0x1F
        self.TIO_COMMAND_QUERY = 0x20
        self.TIO_COMMAND_PROPGET = 0x30
        self.TIO_COMMAND_PROPSET = 0x31

        self.code_to_name = {}
        self.code_to_name[self.TIO_COMMAND_SET] = 'set'
        self.code_to_name[self.TIO_COMMAND_INSERT] = 'insert'
        self.code_to_name[self.TIO_COMMAND_DELETE] = 'delete'
        self.code_to_name[self.TIO_COMMAND_PUSH_BACK] = 'push_back'
        self.code_to_name[self.TIO_COMMAND_PUSH_FRONT] = 'push_front'
        self.code_to_name[self.TIO_COMMAND_POP_BACK] = 'pop_back'
        self.code_to_name[self.TIO_COMMAND_POP_FRONT] = 'pop_front'
        self.code_to_name[self.TIO_COMMAND_CLEAR] = 'clear'
        self.code_to_name[self.TIO_COMMAND_PROPSET] = 'propset'
        

        self.tio_initialize()        

g_tioclientdll = TioClientDll()

def NativeTioDataToPythonType(native_value):
    if type(native_value) is C_TIO_DATA:
        pass
    elif type(native_value) is POINTER(C_TIO_DATA):
        # native null pointer?
        if not native_value:
            return None
        native_value = native_value.contents

    if native_value.data_type == TIO_DATA_TYPE_NONE:
        value = None
        
    elif native_value.data_type == TIO_DATA_TYPE_INT:
        value = native_value.int_
        
    elif native_value.data_type == TIO_DATA_TYPE_DOUBLE:
        value = native_value.double_
        
    elif native_value.data_type == TIO_DATA_TYPE_STRING:
        value = native_value.string_

    else:
        raise Exception('invalid data type on C_TIO_DATA struct')

    return value    

class TioData(object):
    def __init__(self, value=None):
        if type(value) in (C_TIO_DATA, POINTER(C_TIO_DATA)):
            self.__value = NativeTioDataToPythonType(value)
        else:
            self.__value = value

        self.__native = None
        self.__was_byref = False

    def set(self, value):
        if value is not None and type(value) not in (str, int, float, Decimal):
            raise Exception('non supported type: %s' % type(value))

        self.__value = value
        self.__native = None

    def value(self):
        if self.__was_byref:
            self.__value = NativeTioDataToPythonType(self.__native)

        return self.__value                

    def native_byref(self):
        ret = byref(self.native())
        self.__was_byref = True
        return ret

    def native(self):
        if self.__native:	
            return self.__native

        value = self.value()        
        
        if value is None:
            native = C_TIO_DATA()
            g_tioclientdll.tiodata_init(byref(native))
        else:
            t = type(self.__value)
            if t is int:
                native = C_TIO_DATA()
                g_tioclientdll.tiodata_init(byref(native))
                g_tioclientdll.tiodata_set_int(byref(native), value)
            elif t is float:
                native = C_TIO_DATA()
                g_tioclientdll.tiodata_init(byref(native))
                g_tioclientdll.tiodata_set_double(byref(native), value)
            elif t is str:
                native = C_TIO_DATA()
                g_tioclientdll.tiodata_init(byref(native))
                g_tioclientdll.tiodata_set_string(byref(native), self.__value)
            else:
                raise Exception('data type "%s" is not supported by Tio' % t)
        
        self.__native = native

        return native

    def __str__(self):
        value = self.value()
        return '%s (%s)' % (value, type(value))

    def __repr__(self):
        value = self.value()
        return '%s (%s)' % (value, type(value))

def x1_decode(value):
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

x1_type_map = {}
x1_type_map[int] = 'I'
x1_type_map[Decimal] = 'D'
x1_type_map[str] = 'S'
x1_type_map[unicode] = 'S'

def x1_encode(values):
    header_io = StringIO()
    values_io = StringIO()

    header_io.write('X1')
    header_io.write('%04XC' % len(values))

    for v in values:
        strv = str(v)
        header_io.write('%04X%s' % (len(strv), x1_type_map[type(v)]))
        values_io.write(strv) ; values_io.write(' ')

    header_io.write(' ')
    header_io.write(values_io.getvalue())

    encoded_value = header_io.getvalue()
    return encoded_value    


class ContainerPythonizer(object):
    def __del__(self):
        self.close()

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


class TioContainer(ContainerPythonizer):
    def __init__(self, connection, native_container, name):
        assert(type(native_container) is c_void_p)
        self.connection = connection
        self.native_container = native_container
        self.name = name

    def __data_command(self, func, key, value=None, metadata=None):
        result = func(
            self.native_container,
            TioData(key).native_byref(),
            TioData(value).native_byref(),
            TioData(metadata).native_byref())

        self.connection.test_result(result, None)

    def clear(self):
        result = g_tioclientdll.tio_container_clear(
            self.native_container)

        self.connection.test_result(result, None)

    def get(self, searchKey, withKeyAndMetadata=False):
        key = TioData()
        value = TioData()
        metadata = TioData()

        result = g_tioclientdll.tio_container_get(
            self.native_container,
            TioData(searchKey).native_byref(),
            key.native_byref(),
            value.native_byref(),
            metadata.native_byref())

        self.connection.test_result(result, None)

        return value.value() if not withKeyAndMetadata else (key.value(), value.value(), metadata.value())

    def set(self, key, value, metadata=None):
        self.__data_command(
            g_tioclientdll.tio_container_set,
            key,
            value,
            metadata)

    def insert(self, key, value, metadata=None):
        self.__data_command(
            g_tioclientdll.tio_container_insert,
            key,
            value,
            metadata)        

    def delete(self, key):
        self.__data_command(
            g_tioclientdll.tio_container_delete,
            key,
            None,
            metadata)

    def push_back(self, value, metadata=None):
        self.__data_command(
            g_tioclientdll.tio_container_push_back,
            None,
            value,
            metadata)

    def push_front(self, value, metadata=None):
        self.__data_command(
            g_tioclientdll.tio_container_push_front,
            None,
            value,
            metadata)

    def pop_back(self, withKeyAndMetadata=False):
        key = TioData()
        value = TioData()
        metadata = TioData()

        result = g_tioclientdll.tio_container_pop_back(
            self.native_container,
            key.native_byref(),
            value.native_byref(),
            metadata.native_byref())

        self.connection.test_result(result, None)

        return value.value() if not withKeyAndMetadata else (key.value(), value.value(), metadata.value())

    def pop_front(self, withKeyAndMetadata=False):
        key = TioData()
        value = TioData()
        metadata = TioData()

        result = g_tioclientdll.tio_container_pop_front(
            self.native_container,
            key.native_byref(),
            value.native_byref(),
            metadata.native_byref())

        self.connection.test_result(result, None)

        return value.value() if not withKeyAndMetadata else (key.value(), value.value(), metadata.value())

    def get_count(self):
        count = c_int()
        result = g_tioclientdll.tio_container_get_count(self.native_container, byref(count))
        self.connection.test_result(result, None)
        return count.value

    def __query_callback(self, cookie, query_id, key, value, metadata):
        k = TioData(key)
        v = TioData(value)
        m = TioData(metadata)

        if self.current___query_callback:
            self.current___query_callback(self, k.value(), v.value(), m.value())
        
        return 0

    def query_with_callback(self, callback, startOffset=0, endOffset=0):
        self.current___query_callback = callback
        
        result = g_tioclientdll.tio_container_query(
            self.native_container,
            startOffset if startOffset is not None else 0,
            endOffset if endOffset is not None else 0,
            g_tioclientdll.query_callback_t(self.__query_callback),
            123456)
        
        self.connection.test_result(result, None)

    def query(self, startOffset=0, endOffset=0):
        return [x[1] for x in self.query_with_key_and_metadata(startOffset, endOffset)]

    def query_with_key_and_metadata(self, startOffset=0, endOffset=0):
        records = []

        def c(container, key, value, metadata):
            records.append((key, value, metadata))

        self.query_with_callback(c, startOffset, endOffset)

        return records

    def __subscribe_callback(self, cookie, handle, event_code, key, value, metadata):
        k = TioData(key).value()
        v = TioData(value).value()
        m = TioData(metadata).value()
        event_name = g_tioclientdll.code_to_name[event_code]
        
        self.events_callback(self, event_name, k, v, m)

    def subscribe(self, callback, start=0):
        self.events_callback = callback
        self.callback_ref_holder = g_tioclientdll.event_callback_t(self.__subscribe_callback)
        
        result = g_tioclientdll.tio_container_subscribe(
            self.native_container,
            TioData(start).native_byref(),
            self.callback_ref_holder,
            0)

        self.connection.test_result(result, None)

    def unsubscribe(self):
        result = g_tioclientdll.tio_container_usubscribe(
            self.native_container)

        self.connection.test_result(result, None)

        self.events_callback = None
        self.callback_ref_holder = None
        

class TioServerConnection(object):
    def __init__(self, address, port):
        self.cn = c_void_p()
        self.containers = []
        self.connect(address, port)

    def __get_cn(self):
        if self.cn.value is None:
            raise Exception('not connected')
        return self.cn

    def __del__(self):
        g_tioclientdll.tio_disconnect(self.__get_cn())

    def test_result(self, result, response):
        if result < 0:
            raise Exception()
    
    def connect(self, host, port):
        result = g_tioclientdll.tio_connect(host, port, byref(self.cn))
        self.test_result(result, None)

    def open(self, name, type = None):
        native_container = c_void_p()

        result = g_tioclientdll.tio_open(self.__get_cn(), name, type, byref(native_container))
        self.test_result(result, None)

        container = TioContainer(self, native_container, name)
        self.containers.append(container)

        return container

    def create(self, name, type):
        native_container = c_void_p()

        result = g_tioclientdll.tio_create(self.__get_cn(), name, type, byref(native_container))
        self.test_result(result, None)

        container = TioContainer(self, native_container, name)
        self.containers.append(container)

        return container


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
