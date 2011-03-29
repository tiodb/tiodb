#include "../tioclient.c"


void TEST_pr1_message()
{
	struct PR1_MESSAGE* pr1_message;
	int i = 10;
	double d = 1.1;
	char str[] = "test string";
	void* buffer;
	unsigned int buffer_size;

	pr1_message = pr1_message_new();

	pr1_message_add_field(pr1_message, MESSAGE_FIELD_ID_VALUE, MESSAGE_FIELD_TYPE_INT, &i, sizeof(i));
	pr1_message_add_field(pr1_message, MESSAGE_FIELD_ID_KEY, MESSAGE_FIELD_TYPE_STRING, str, strlen(str));
	pr1_message_add_field(pr1_message, MESSAGE_FIELD_ID_METADATA, MESSAGE_FIELD_TYPE_DOUBLE, &d, sizeof(d));

	pr1_message_get_buffer(pr1_message, &buffer, &buffer_size);

	pr1_message_parse(pr1_message);

	assert(pr1_message->field_array[0]->data_type == MESSAGE_FIELD_TYPE_INT);
	assert(pr1_message->field_array[1]->data_type == MESSAGE_FIELD_TYPE_STRING);
	assert(pr1_message->field_array[2]->data_type == MESSAGE_FIELD_TYPE_DOUBLE);

	pr1_message_delete(pr1_message);
}

const char* event_code_to_string(unsigned int event_code)
{
	switch(event_code)
	{
		case TIO_COMMAND_PING: return "TIO_COMMAND_PING(0x10)";
		case TIO_COMMAND_OPEN: return "TIO_COMMAND_OPEN(0x11)";
		case TIO_COMMAND_CREATE: return "TIO_COMMAND_CREATE(0x12)";
		case TIO_COMMAND_CLOSE: return "TIO_COMMAND_CLOSE(0x13)";
		case TIO_COMMAND_SET: return "TIO_COMMAND_SET(0x14)";
		case TIO_COMMAND_INSERT: return "TIO_COMMAND_INSERT(0x15)";
		case TIO_COMMAND_DELETE: return "TIO_COMMAND_DELETE(0x16)";
		case TIO_COMMAND_PUSH_BACK: return "TIO_COMMAND_PUSH_BACK(0x17)";
		case TIO_COMMAND_PUSH_FRONT: return "TIO_COMMAND_PUSH_FRONT(0x18)";
		case TIO_COMMAND_POP_BACK: return "TIO_COMMAND_POP_BACK(0x19)";
		case TIO_COMMAND_POP_FRONT: return "TIO_COMMAND_POP_FRONT(0x1A)";
		case TIO_COMMAND_CLEAR: return "TIO_COMMAND_CLEAR(0x1B)";
		case TIO_COMMAND_COUNT: return "TIO_COMMAND_COUNT(0x1C)";
		case TIO_COMMAND_GET: return "TIO_COMMAND_GET(0x1D)";
		case TIO_COMMAND_SUBSCRIBE: return "TIO_COMMAND_SUBSCRIBE(0x1E)";
		case TIO_COMMAND_UNSUBSCRIBE: return "TIO_COMMAND_UNSUBSCRIBE(0x1F)";
		case TIO_COMMAND_QUERY: return "TIO_COMMAND_QUERY(0x20)";
		default: return "UNKNOWN_COMMAND";
	}
}

void test_event_callback(void* cookie, unsigned int handle, unsigned int event_code, const struct TIO_DATA* key, const struct TIO_DATA* value, const struct TIO_DATA* metadata)
{
	struct TIO_DATA k, v, m;
	tiodata_init(&k); tiodata_init(&v); tiodata_init(&m); 
	tiodata_copy(key, &k); tiodata_copy(value, &v); tiodata_copy(metadata, &m);

	tiodata_convert_to_string(&k);
	tiodata_convert_to_string(&v);
	tiodata_convert_to_string(&m);
	
	printf("cookie: %d, handle: %d, event_code: %s, key: %s, value: %s, metadata: %s\n", 
		cookie, 
		handle, 
		event_code_to_string(event_code),
		k.string_,
		v.string_,
		m.string_);

	tiodata_set_as_none(&k); tiodata_set_as_none(&v); tiodata_set_as_none(&m); 
}

int TEST_metacontainer_info(struct TIO_CONNECTION* connection)
{
	int result;
	struct TIO_CONTAINER* meta_containers = NULL;
	struct TIO_DATA search_key, key, value, metadata;

	tiodata_init(&search_key);
	tiodata_init(&key);
	tiodata_init(&value);
	tiodata_init(&metadata);

	tiodata_init(&search_key);

	result = tio_open(connection, "meta/containers", NULL, &meta_containers);
	if(TIO_FAILED(result))
		goto clean_up_and_return;

	//
	// get metadata container meta information
	//
	tiodata_set_string(&search_key, "meta/containers");

	result = tio_container_get(meta_containers, &search_key, &key, &value, &metadata);
	if(TIO_FAILED(result))
		goto clean_up_and_return;

	assert(tiodata_get_type(&value) == TIO_DATA_TYPE_STRING);
	assert(strcmp(value.string_, "volatile_map") == 0);

clean_up_and_return:
	tiodata_set_as_none(&search_key);
	tiodata_set_as_none(&key);
	tiodata_set_as_none(&value);
	tiodata_set_as_none(&metadata);

	tio_close(meta_containers);

	return result;
}

int TEST_list(struct TIO_CONNECTION* connection)
{
	struct TIO_CONTAINER* test_container = NULL;
	struct TIO_DATA search_key, key, value, metadata;
	int a;
	int result;

	tiodata_init(&search_key);
	tiodata_init(&key);
	tiodata_init(&value);
	tiodata_init(&metadata);

	//
	// create test container, add items
	//
	result = tio_create(connection, "test_list", "volatile_list", &test_container);
	if(TIO_FAILED(result)) goto clean_up_and_return;

	result = tio_container_subscribe(test_container, NULL, &test_event_callback, NULL);
	if(TIO_FAILED(result)) goto clean_up_and_return;

	result = tio_container_clear(test_container);
	if(TIO_FAILED(result)) goto clean_up_and_return;

	for(a = 0 ; a < 50 ; a++)
	{
		tiodata_set_int(&value, a);
		result = tio_container_push_back(test_container, NULL, &value, NULL);
	}

	//
	// check if the count is ok
	//
	result = tio_container_get_count(test_container, &a);
	if(TIO_FAILED(result))
		goto clean_up_and_return;

	assert(a == 50);


	//
	// delete first item and checks
	//
	tiodata_set_int(&search_key, 0);

	result = tio_container_delete(test_container, &search_key);
	if(TIO_FAILED(result))
		goto clean_up_and_return;

	result = tio_container_get(test_container, &search_key, &key, &value, &metadata);
	if(TIO_FAILED(result))
		goto clean_up_and_return;

	assert(tiodata_get_type(&key) == TIO_DATA_TYPE_INT);
	assert(key.int_ == 0);

	assert(tiodata_get_type(&value) == TIO_DATA_TYPE_INT);
	assert(value.int_ == 1);

	//
	// insert first item to restore container to previous state
	//
	tiodata_set_int(&key, 0);
	tiodata_set_int(&value, 0);
	tiodata_set_string(&metadata, "not the first one");

	result = tio_container_insert(test_container, &key, &value, NULL);
	if(TIO_FAILED(result))
		goto clean_up_and_return;

	//
	// unsubscribe and subscribe again
	//
	result = tio_container_unsubscribe(test_container);
	if(TIO_FAILED(result))
		goto clean_up_and_return;

	tiodata_set_int(&value, 0);

	result = tio_container_subscribe(test_container, &value, &test_event_callback, NULL);
	if(TIO_FAILED(result))
		goto clean_up_and_return;

	for(a = 0 ; a < 50 ; a++)
	{
		tiodata_set_int(&search_key, a);

		result = tio_container_get(test_container, &search_key, &key, &value, &metadata);
		if(TIO_FAILED(result)) goto clean_up_and_return;

		assert(tiodata_get_type(&key) == TIO_DATA_TYPE_INT);
		assert(value.int_ == a);

		assert(tiodata_get_type(&value) == TIO_DATA_TYPE_INT);
		assert(value.int_ == a);
	}

	tio_dispatch_pending_events(connection, 0xFFFFFFFF);

clean_up_and_return:
	tiodata_set_as_none(&search_key);
	tiodata_set_as_none(&key);
	tiodata_set_as_none(&value);
	tiodata_set_as_none(&metadata);

	tio_close(test_container);

	return 0;
}




int TEST_map(struct TIO_CONNECTION* connection)
{
	struct TIO_CONTAINER* test_container = NULL;
	struct TIO_DATA search_key, key, value, metadata;
	char* buffer;
	int a;
	int result;

	tiodata_init(&search_key);
	tiodata_init(&key);
	tiodata_init(&value);
	tiodata_init(&metadata);

	//
	// create test container, add items
	//
	result = tio_create(connection, "test_map", "volatile_map", &test_container);
	if(TIO_FAILED(result)) goto clean_up_and_return;

	result = tio_container_subscribe(test_container, NULL, &test_event_callback, NULL);
	if(TIO_FAILED(result)) goto clean_up_and_return;

	result = tio_container_clear(test_container);
	if(TIO_FAILED(result)) goto clean_up_and_return;

	for(a = 0 ; a < 50 ; a++)
	{
		buffer = tiodata_set_string_get_buffer(&key, 64);
		sprintf(buffer,"%d", a);

		tiodata_set_int(&value, a);
		result = tio_container_set(test_container, &key, &value, NULL);
	}

	//
	// check if the count is ok
	//
	result = tio_container_get_count(test_container, &a);
	if(TIO_FAILED(result))
		goto clean_up_and_return;

	assert(a == 50);


	//
	// delete first item and checks
	//
	tiodata_set_string(&search_key, "0");

	result = tio_container_delete(test_container, &search_key);
	if(TIO_FAILED(result))
		goto clean_up_and_return;

	tiodata_set_string(&search_key, "1");
	result = tio_container_get(test_container, &search_key, &key, &value, &metadata);
	if(TIO_FAILED(result))
		goto clean_up_and_return;

	assert(tiodata_get_type(&key) == TIO_DATA_TYPE_STRING);
	assert(strcmp(key.string_, "1") == 0);

	assert(tiodata_get_type(&value) == TIO_DATA_TYPE_INT);
	assert(value.int_ == 1);

	//
	// insert first item to restore container to previous state
	//
	tiodata_set_string(&key, "0");
	tiodata_set_int(&value, 0);
	tiodata_set_string(&metadata, "not the first one");

	result = tio_container_set(test_container, &key, &value, NULL);
	if(TIO_FAILED(result))
		goto clean_up_and_return;

	//
	// unsubscribe and subscribe again
	//
	result = tio_container_unsubscribe(test_container);
	if(TIO_FAILED(result))
		goto clean_up_and_return;

	tiodata_set_int(&value, 0);

	result = tio_container_subscribe(test_container, &value, &test_event_callback, NULL);
	if(TIO_FAILED(result))
		goto clean_up_and_return;

	for(a = 0 ; a < 50 ; a++)
	{
		buffer = tiodata_set_string_get_buffer(&search_key, 64);
		sprintf(buffer, "%d", a);

		result = tio_container_get(test_container, &search_key, &key, &value, &metadata);
		if(TIO_FAILED(result)) goto clean_up_and_return;

		assert(tiodata_get_type(&key) == TIO_DATA_TYPE_STRING);

		assert(tiodata_get_type(&value) == TIO_DATA_TYPE_INT);
		assert(value.int_ == a);
	}


	tio_dispatch_pending_events(connection, 0xFFFFFFFF);

clean_up_and_return:
	tiodata_set_as_none(&search_key);
	tiodata_set_as_none(&key);
	tiodata_set_as_none(&value);
	tiodata_set_as_none(&metadata);

	tio_close(test_container);

	return 0;
}








int main()
{
	int result;
	struct TIO_CONNECTION* connection = NULL;

	tio_initialize();

	result = tio_connect("127.0.0.1", 6666, &connection);
	if(TIO_FAILED(result))
		goto clean_up_and_return;

	result = tio_ping(connection, "asdasdasdasdASDASDASDASDasdasdASDASDPOIYQFOHPAS*(FY(#RPSACTASC");
	if(TIO_FAILED(result))
		goto clean_up_and_return;

	TEST_pr1_message();

	TEST_list(connection);

	TEST_map(connection);

clean_up_and_return:
	tio_disconnect(connection);
	return 0;
}
