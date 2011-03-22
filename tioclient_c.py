from ctypes import *

"""
struct TIO_DATA
{	
	unsigned int data_type;
	int int_;
	char* string_;
	double double_;
};
"""
class TIO_DATA(Structure):
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

		# tio_connect(const char* host, short port, struct TIO_CONNECTION** connection);;
		self.tio_connect = self.dll.tio_connect
		self.tio_connect.argtypes = [c_char_p, c_short, POINTER(c_void_p)]

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
		
		# int tio_container_get(struct TIO_CONTAINER* container, const struct TIO_DATA* search_key, struct TIO_DATA* key, struct TIO_DATA* value, struct TIO_DATA* metadata);
		self.tio_container_get = self.dll.tio_container_get
		self.tio_container_get.argtypes = [c_void_p, POINTER(TIO_DATA), POINTER(TIO_DATA), POINTER(TIO_DATA), POINTER(TIO_DATA)]
		
		# int tio_container_get_count(struct TIO_CONTAINER* container, int* count);
		self.tio_container_get_count = self.dll.tio_container_get_count
		self.tio_container_get_count.argtypes = [c_void_p, POINTER(c_int)]
		
		container_funcs = [\
			'tio_container_clear',
			'tio_container_unsbscribe',
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
			self.__dict__[f] = func
			
		for func_name in container_key_funcs:
			func = getattr(self.dll, func_name)
			func.argtypes = [c_void_p, POINTER(TIO_DATA)]
			self.__dict__[f] = func
			
		for func_name in container_key_value_funcs:
			func = getattr(self.dll, func_name)
			func.argtypes = [c_void_p, POINTER(TIO_DATA), POINTER(TIO_DATA)]
			self.__dict__[f] = func
			
		for func_name in container_key_value_metadata_funcs:
			func = getattr(self.dll, func_name)
			func.argtypes = [c_void_p, POINTER(TIO_DATA), POINTER(TIO_DATA), POINTER(TIO_DATA)]
			self.__dict__[f] = func
			
		# typedef void (*event_callback_t)(void* /*cookie*/, unsigned int, unsigned int, const struct TIO_DATA*, const struct TIO_DATA*, const struct TIO_DATA*);
		self.event_callback_t = CFUNCTYPE(c_int, c_void_p, c_uint, c_uint, POINTER(TIO_DATA), POINTER(TIO_DATA), POINTER(TIO_DATA))
		
		# typedef void (*query_callback_t)(void* /*cookie*/, unsigned int /*queryid*/, const struct TIO_DATA*, const struct TIO_DATA*, const struct TIO_DATA*);
		self.query_callback_t = CFUNCTYPE(c_int, c_void_p, c_uint, POINTER(TIO_DATA), POINTER(TIO_DATA), POINTER(TIO_DATA))

		
		# int tio_container_query(struct TIO_CONTAINER* container, int start, int end, query_callback_t query_callback, void* cookie);
		self.tio_container_query = self.dll.tio_container_query
		self.tio_container_query.argtypes = [c_void_p, c_int, c_int, self.query_callback_t, c_void_p]
		
		
		# int tio_container_subscribe(struct TIO_CONTAINER* container, struct TIO_DATA* start, event_callback_t event_callback, void* cookie);
		self.tio_container_subscribe = self.dll.tio_container_subscribe
		self.tio_container_subscribe.argtypes = [c_void_p, POINTER(TIO_DATA), self.event_callback_t, c_void_p]
		
		
		# tio_dispatch_pending_events(struct TIO_CONNECTION* connection, unsigned int max_events);
		self.tio_dispatch_pending_events = self.dll.tio_dispatch_pending_events
		self.tio_dispatch_pending_events.argtypes = [c_void_p, c_uint]
		

		# tiodata_init(struct TIO_DATA* tiodata);
		self.tiodata_init = self.dll.tiodata_init
		self.tiodata_init.argtypes = [POINTER(TIO_DATA),]
		
		# int tiodata_get_type(struct TIO_DATA* tiodata);
		self.tiodata_get_type = self.dll.tiodata_get_type
		self.tiodata_get_type.argtypes = [POINTER(TIO_DATA),]
		
		# tiodata_set_as_none(struct TIO_DATA* tiodata);
		self.tiodata_set_as_none = self.dll.tiodata_set_as_none
		self.tiodata_set_as_none.argtypes = [POINTER(TIO_DATA),]
		
		# tiodata_set_string_get_buffer(struct TIO_DATA* tiodata, unsigned int min_size);
		self.tiodata_set_string_get_buffer = self.dll.tiodata_set_string_get_buffer
		self.tiodata_set_string_get_buffer.argtypes = [POINTER(TIO_DATA), c_uint]
		
		# tiodata_set_string(struct TIO_DATA* tiodata, const char* value);
		self.tiodata_set_string = self.dll.tiodata_set_string
		self.tiodata_set_string.argtypes = [POINTER(TIO_DATA), c_char_p]
		
		# tiodata_set_int(struct TIO_DATA* tiodata, int value);
		self.tiodata_set_int = self.dll.tiodata_set_int
		self.tiodata_set_int.argtypes = [POINTER(TIO_DATA), c_int]
		
		# tiodata_set_double(struct TIO_DATA* tiodata, double value);
		self.tiodata_set_double = self.dll.tiodata_set_double
		self.tiodata_set_double.argtypes = [POINTER(TIO_DATA), c_double]
		

g_tioclientdll = TioClientDll()


