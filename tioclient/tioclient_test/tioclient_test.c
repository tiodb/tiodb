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

void test_event_callback(void* cookie, unsigned int handle, unsigned int event_code, const struct TIO_DATA* key, const struct TIO_DATA* value, const struct TIO_DATA* metadata)
{
	cookie; handle; key; value; metadata;
	printf("event code: %d\n", event_code);
}


int TEST_tio()
{
	struct TIO_CONNECTION* connection = NULL;
	struct TIO_CONTAINER* meta_containers = NULL;
	struct TIO_CONTAINER* test_container = NULL;
	struct TIO_DATA search_key, key, value, metadata;
	int a;
	int result;

	tio_initialize();

	tiodata_init(&search_key);
	tiodata_init(&key);
	tiodata_init(&value);
	tiodata_init(&metadata);

	result = tio_connect("127.0.0.1", 6666, &connection);
	if(TIO_FAILED(result))
		goto clean_up_and_return;

	result = tio_ping(connection, "asdasdasdasdASDASDASDASDasdasdASDASDPOIYQFOHPAS*(FY(#RPSACTASC");
	if(TIO_FAILED(result))
		goto clean_up_and_return;

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

	//
	// create test container, add items
	//
	result = tio_create(connection, "test_container", "volatile_list", &test_container);
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

	//
	// not dispatching all pending events, to check if disconnect() code
	// is releasing them
	//
	tio_dispatch_pending_events(connection, 50);

clean_up_and_return:
	tiodata_set_as_none(&search_key);
	tiodata_set_as_none(&key);
	tiodata_set_as_none(&value);
	tiodata_set_as_none(&metadata);

	tio_close(meta_containers);
	tio_close(test_container);
	tio_disconnect(connection);

	return 0;
}


int main()
{
	TEST_pr1_message();

	TEST_tio();
}
