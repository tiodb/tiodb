#include "tioclient_internals.h"
#include "tioclient.h"

int socket_send(SOCKET socket, const void* buffer, unsigned int len)
{
	int ret = send(socket, (const char*)buffer, len, 0);

	if(ret <= 0)
		ret = TIO_ERROR_NETWORK;
	
	return ret;
}

int socket_receive(SOCKET socket, void* buffer, int len)
{
	int ret = 0;
	char* char_buffer = (char*)buffer;
	int received = 0;

#ifdef _DEBUG
	memset(char_buffer, 0xFF, len);
#endif

	while(received < len)
	{
		//
		// Windows supports MSG_WAITALL only on Windows Server 2008 or superior... :-(
		// So I need to emulate it here
		//
		ret = recv(socket, char_buffer + received, len - received, 0);

		if(ret <= 0)
			return TIO_ERROR_NETWORK;

		received += ret;
	}

	assert(ret < 0 || received == len);
		
	return ret;
}

int socket_receive_if_available(SOCKET socket, void* buffer, unsigned int len)
{
	int ret;
#ifdef _WIN32
	u_long pending_bytes = 0;

	ret = ioctlsocket(socket, FIONREAD, &pending_bytes);
	if(ret == SOCKET_ERROR)
		return ret;

	if(pending_bytes < len)
		return 0;

	return socket_receive(socket, buffer, len); 
#else
	ret = recv(socket, (char*)buffer, len, MSG_DONTWAIT);

	// nothing pending
	if(ret == EAGAIN)
		return 0;

	return ret;
#endif
}




struct STREAM_BUFFER* stream_buffer_new()
{
	struct STREAM_BUFFER* message_buffer = (struct STREAM_BUFFER*)malloc(sizeof(struct STREAM_BUFFER));

	message_buffer->buffer_size = 1024 * 4;
	message_buffer->buffer = (char*) malloc(message_buffer->buffer_size);
	message_buffer->current = message_buffer->buffer;

	return message_buffer;
}

unsigned int stream_buffer_space_used(struct STREAM_BUFFER* stream_buffer)
{
	return stream_buffer->current - stream_buffer->buffer;
}

unsigned int stream_buffer_space_left(struct STREAM_BUFFER* stream_buffer)
{
	return stream_buffer->buffer_size - stream_buffer_space_used(stream_buffer);
}

void stream_buffer_ensure_space_left(struct STREAM_BUFFER* stream_buffer, unsigned int size)
{
	unsigned int new_size;
	char* new_buffer;
	unsigned int used = stream_buffer_space_used(stream_buffer);

	if(stream_buffer->buffer_size - used >= size)
		return;

	// If no room to the new data, we'll raise the the stream size by
	// (new data size) * 2. Not sure if it's the best heuristic, but
	// surely works
	new_size = stream_buffer->buffer_size + (size * 2);
	new_buffer = (char*)malloc(new_size);
		
	memcpy(new_buffer, stream_buffer->buffer, stream_buffer->buffer_size);

	stream_buffer->buffer = new_buffer;
	stream_buffer->buffer_size = new_size;
	stream_buffer->current = stream_buffer->buffer + used;
}

unsigned int stream_buffer_seek(struct STREAM_BUFFER* stream_buffer, unsigned int position)
{
	if(position >= stream_buffer->buffer_size)
		position = stream_buffer->buffer_size - 1;

	stream_buffer->current = stream_buffer->buffer + position;

	return position;
}


//
// this function will reserve the space on stream and return the pointer. It's useful for
// when you want to add a struct to the stream but you should fill the variables. This will
// avoid creating the struct on stack and copying the memory to the stream. Something like:
//
// struct MY_STRUCT* struct = (struct MY_STRCT*)stream_buffer_get_write_pointer(sb, sizeof(struct MY_STRUCT);
// struct->my_int = 10;
//
void* stream_buffer_get_write_pointer(struct STREAM_BUFFER* stream_buffer, unsigned int size)
{
	void* write_pointer;

	stream_buffer_ensure_space_left(stream_buffer, size);

	write_pointer = stream_buffer->current;

	// reserve the space
	stream_buffer->current += size;

	return write_pointer;
}

void stream_buffer_write(struct STREAM_BUFFER* stream_buffer, const void* buffer, unsigned int size)
{
	stream_buffer_ensure_space_left(stream_buffer, size);

	memcpy(stream_buffer->current, buffer, size);

	stream_buffer->current += size;
}

unsigned int stream_buffer_read(struct STREAM_BUFFER* stream_buffer, void* buffer, unsigned int size)
{
	unsigned int left = stream_buffer_space_left(stream_buffer);
	
	if(size > left)
		size = left;

	memcpy(buffer, stream_buffer->buffer, size);

	return size;
}


void stream_buffer_delete(struct STREAM_BUFFER* message_buffer)
{
	free(message_buffer->buffer);
	free(message_buffer);
}




















struct PR1_MESSAGE* pr1_message_new()
{
	struct PR1_MESSAGE_HEADER* header;
	struct PR1_MESSAGE* pr1_message = (struct PR1_MESSAGE*)malloc(sizeof(struct PR1_MESSAGE));
	
	pr1_message->stream_buffer = stream_buffer_new();
	pr1_message->field_array = NULL;

	header = (struct PR1_MESSAGE_HEADER*)stream_buffer_get_write_pointer(pr1_message->stream_buffer, sizeof(struct PR1_MESSAGE_HEADER));

	// mark size on message header as invalid
	header->message_size = 0xFFFFFFFF;
	header->reserved = 0;

	pr1_message->field_count = 0;

	return pr1_message;
}

void pr1_message_delete(struct PR1_MESSAGE* pr1_message)
{
	if(!pr1_message)
		return;

	stream_buffer_delete(pr1_message->stream_buffer);

	free(pr1_message->field_array);

	free(pr1_message);
}

void pr1_message_add_field(struct PR1_MESSAGE* pr1_message, unsigned short field_id, unsigned short data_type, const void* buffer, unsigned int buffer_size)
{
	struct PR1_MESSAGE_FIELD_HEADER* field_header = 
		(struct PR1_MESSAGE_FIELD_HEADER*)
			stream_buffer_get_write_pointer(pr1_message->stream_buffer, 
				sizeof(struct PR1_MESSAGE_FIELD_HEADER));

	field_header->data_type = data_type;
	field_header->field_id = field_id;
	field_header->data_size = buffer_size;

	stream_buffer_write(pr1_message->stream_buffer, buffer, buffer_size);

	pr1_message->field_count++;
}

void pr1_message_add_field_int(struct PR1_MESSAGE* pr1_message, unsigned short field_id, int value)
{
	pr1_message_add_field(pr1_message, field_id, MESSAGE_FIELD_TYPE_INT, &value, sizeof(value));
}

void pr1_message_add_field_double(struct PR1_MESSAGE* pr1_message, unsigned short field_id, double value)
{
	pr1_message_add_field(pr1_message, field_id, MESSAGE_FIELD_TYPE_DOUBLE, &value, sizeof(value));
}

void pr1_message_add_field_string(struct PR1_MESSAGE* pr1_message, unsigned short field_id, const char* value)
{
	pr1_message_add_field(pr1_message, field_id, MESSAGE_FIELD_TYPE_STRING, value, strlen(value));
}

int pr1_message_field_get_int(const struct PR1_MESSAGE_FIELD_HEADER* field)
{
	assert(field->data_size == sizeof(int));
	field++;
	return *(int*)field;
}

double pr1_message_field_get_double(const struct PR1_MESSAGE_FIELD_HEADER* field)
{
	assert(field->data_size == sizeof(double));
	field++;
	return *(double*)field;
}

const void* pr1_message_field_get_buffer(const struct PR1_MESSAGE_FIELD_HEADER* field)
{
	field++;
	return field;
}

void pr1_message_field_get_string(const struct PR1_MESSAGE_FIELD_HEADER* field, char** buffer, unsigned int buffer_size)
{
	if(buffer_size > field->data_size + 1)
		buffer_size = field->data_size;

	field++;

	memcpy(buffer, field, buffer_size);

	buffer[buffer_size] = '\0';
}

void pr1_message_get_buffer(struct PR1_MESSAGE* pr1_message, void** buffer, unsigned int* size)
{
	struct PR1_MESSAGE_HEADER* header = (struct PR1_MESSAGE_HEADER*)pr1_message->stream_buffer->buffer;
	header->message_size = stream_buffer_space_used(pr1_message->stream_buffer) - sizeof(struct PR1_MESSAGE_HEADER);
	header->field_count = pr1_message->field_count;

	*buffer = pr1_message->stream_buffer->buffer;
	*size = stream_buffer_space_used(pr1_message->stream_buffer);
}

void pr1_message_parse(struct PR1_MESSAGE* pr1_message)
{
	unsigned int a;
	struct PR1_MESSAGE_HEADER* header = (struct PR1_MESSAGE_HEADER*)pr1_message->stream_buffer->buffer;
	char* current = (char*)&header[1];

	pr1_message->field_count = header->field_count;

	free(pr1_message->field_array);
	
	pr1_message->field_array = (struct PR1_MESSAGE_FIELD_HEADER**) malloc(header->field_count * sizeof(void*));

	for(a = 0 ; a < pr1_message->field_count ; a++)
	{
		pr1_message->field_array[a] = (struct PR1_MESSAGE_FIELD_HEADER*)current;
		current += pr1_message->field_array[a]->data_size + sizeof(struct PR1_MESSAGE_FIELD_HEADER);
	}
}

struct PR1_MESSAGE_FIELD_HEADER* pr1_message_field_find_by_id(const struct PR1_MESSAGE* pr1_message, unsigned int field_id)
{
	unsigned int a;

	for(a = 0 ; a < pr1_message->field_count ; a++)
		if(pr1_message->field_array[a]->field_id == field_id)
			return pr1_message->field_array[a];

	return NULL;
}



int pr1_message_send(SOCKET socket, struct PR1_MESSAGE* pr1_message)
{
	void* buffer;
	unsigned int size;

	pr1_message_get_buffer(pr1_message, &buffer, &size);

	return socket_send(socket, buffer, size);
}

//
// this function will delete the message EVEN IF IT WAS NOT SEND
//
int pr1_message_send_and_delete(SOCKET socket, struct PR1_MESSAGE* pr1_message)
{
	int result;
	result = pr1_message_send(socket, pr1_message);

	pr1_message_delete(pr1_message);

	return result;
}

struct PR1_MESSAGE* pr1_message_new_get_buffer_for_receive(struct PR1_MESSAGE_HEADER* message_header, void** buffer)
{
	struct PR1_MESSAGE* pr1_message = pr1_message_new();

	stream_buffer_seek(pr1_message->stream_buffer, 0);

	stream_buffer_write(pr1_message->stream_buffer, message_header, sizeof(struct PR1_MESSAGE_HEADER));

	*buffer = stream_buffer_get_write_pointer(pr1_message->stream_buffer, message_header->message_size);

	return pr1_message;
}

int pr1_message_receive(SOCKET socket, struct PR1_MESSAGE** pr1_message)
{
	int result;
	struct PR1_MESSAGE_HEADER pr1_message_header;
	void* receive_buffer;
	
	result = socket_receive(socket, &pr1_message_header, sizeof(struct PR1_MESSAGE_HEADER));

	if(TIO_FAILED(result))
		return result;

	*pr1_message = pr1_message_new_get_buffer_for_receive(&pr1_message_header, &receive_buffer);

	result = socket_receive(
		socket, 
		receive_buffer,
		pr1_message_header.message_size);

	if(TIO_FAILED(result))
	{
		pr1_message_delete(*pr1_message);
		*pr1_message = NULL;
		return result;
	}

	pr1_message_parse(*pr1_message);

	return result;
}


int pr1_message_receive_if_available(SOCKET socket, struct PR1_MESSAGE** pr1_message)
{
	int result;
	struct PR1_MESSAGE_HEADER pr1_message_header;
	void* receive_buffer;

	result = socket_receive_if_available(socket, &pr1_message_header, sizeof(struct PR1_MESSAGE_HEADER));

	if(result == 0)
		return 0;

	if(TIO_FAILED(result))
		return result;

	*pr1_message = pr1_message_new_get_buffer_for_receive(&pr1_message_header, &receive_buffer);

	result = socket_receive(
		socket, 
		receive_buffer,
		pr1_message_header.message_size);

	if(TIO_FAILED(result))
	{
		pr1_message_delete(*pr1_message);
		*pr1_message = NULL;
		return result;
	}

	pr1_message_parse(*pr1_message);

	return result;
}











































void tiodata_init(struct TIO_DATA* tiodata)
{
	memset(tiodata, 0, sizeof(struct TIO_DATA));
	tiodata->data_type = TIO_DATA_TYPE_NONE;
}

unsigned int tiodata_get_type(struct TIO_DATA* tiodata)
{
	return tiodata->data_type;
}

void tiodata_set_as_none(struct TIO_DATA* tiodata)
{
	if(!tiodata)
		return;

	if(tiodata_get_type(tiodata) == TIO_DATA_TYPE_STRING)
	{
		free(tiodata->string_);
		tiodata->string_ = NULL;
		tiodata->string_size_ = 0;
	}

	tiodata->data_type = TIO_DATA_TYPE_NONE;
}


char* tiodata_string_get_buffer(struct TIO_DATA* tiodata, unsigned int min_size)
{
	if(tiodata->data_type == TIO_DATA_TYPE_STRING && tiodata->string_size_ >= min_size)
		return tiodata->string_;

	tiodata_set_as_none(tiodata);
	tiodata->data_type = TIO_DATA_TYPE_STRING;
	tiodata->string_size_ = min_size + 1;
	tiodata->string_ = (char*)malloc(tiodata->string_size_);

	return tiodata->string_;
}

void tiodata_string_release_buffer(struct TIO_DATA* tiodata)
{
	unsigned int a;
	
	if(tiodata->data_type != TIO_DATA_TYPE_STRING)
		return;

	for(a = 0 ; a < tiodata->string_size_ ; a++) 
	{
		if(tiodata->string_[a] == '\0')
		{
			tiodata->string_size_ = a;
			break;
		}
	}
}


void tiodata_set_string(struct TIO_DATA* tiodata, const char* value)
{
	tiodata_set_string_and_size(tiodata, value, strlen(value));
}

void tiodata_set_string_and_size(struct TIO_DATA* tiodata, const void* buffer, unsigned int len)
{
	tiodata_set_as_none(tiodata);

	tiodata->data_type = TIO_DATA_TYPE_STRING;

	tiodata->string_size_ = len;
	tiodata->string_ = (char*)malloc(tiodata->string_size_ + 1);
	memcpy(tiodata->string_, buffer, tiodata->string_size_);

	//
	// Tio string can have a \0 inside. But we're going to add a \0 to
	// the end, just in case
	//
	tiodata->string_[tiodata->string_size_] = '\0';
}

void tiodata_set_int(struct TIO_DATA* tiodata, int value)
{
	tiodata_set_as_none(tiodata);

	tiodata->data_type = TIO_DATA_TYPE_INT;

	tiodata->int_ = value;
}

void tiodata_set_double(struct TIO_DATA* tiodata, double value)
{
	tiodata_set_as_none(tiodata);

	tiodata->data_type = TIO_DATA_TYPE_DOUBLE;

	tiodata->double_ = value;
}

void tiodata_copy(const struct TIO_DATA* source, struct TIO_DATA* destination)
{
	tiodata_set_as_none(destination);

	switch(source->data_type)
	{
	case TIO_DATA_TYPE_NONE:
	case TIO_DATA_TYPE_INT:
	case TIO_DATA_TYPE_DOUBLE:
		*destination = *source;
		break;
	case TIO_DATA_TYPE_STRING:
		tiodata_set_string_and_size(destination, source->string_, source->string_size_);
		break;
	};
}

void tiodata_convert_to_string(struct TIO_DATA* tiodata)
{
	char buffer[64]; // 64 bytes will ALWAYS be enough. I'm sure.

	if(!tiodata)
		return;

	if(tiodata->data_type == TIO_DATA_TYPE_STRING)
		return;

	switch(tiodata->data_type)
	{
	case TIO_DATA_TYPE_NONE:
		strcpy(buffer, "[NONE]");
		break;
	case TIO_DATA_TYPE_INT:
		sprintf(buffer, "%d", tiodata->int_);
		break;
	case TIO_DATA_TYPE_DOUBLE:
		sprintf(buffer, "%g", tiodata->double_);
		break;
	};

	buffer[sizeof(buffer)-1] = '\0';

	tiodata_set_string(tiodata, buffer);
}












int tio_connect(const char* host, short port, struct TIO_CONNECTION** connection)
{
	SOCKET sockfd;
	struct sockaddr_in serv_addr;
	struct hostent *server;
	int result;
	char buffer[sizeof("going binary") -1];

	sockfd = socket(AF_INET, SOCK_STREAM, 0);
	if (sockfd < 0) 
		return TIO_ERROR_NETWORK;

	server = gethostbyname(host);

	if (server == NULL) {
		return TIO_ERROR_NETWORK;
	}

	serv_addr.sin_addr.s_addr=*((unsigned long*)server->h_addr);
	serv_addr.sin_family=AF_INET;
	serv_addr.sin_port = htons(port);

	if (connect(sockfd,(struct sockaddr *) &serv_addr,sizeof(serv_addr)) < 0)
		return TIO_ERROR_NETWORK;

	result = socket_send(sockfd, "protocol binary\r\n", sizeof("protocol binary\r\n") -1);
	if(TIO_FAILED(result)) 
	{
		closesocket(sockfd);
		return result;
	}

	result = socket_receive(sockfd, buffer, sizeof(buffer));
	if(TIO_FAILED(result)) 
	{
		closesocket(sockfd);
		return result;
	}

	// invalid answer
	if(memcmp(buffer, "going binary", sizeof("going binary")-1) !=0)
	{
		closesocket(sockfd);
		return TIO_ERROR_PROTOCOL;
	}

#ifdef _WIN32
	result = 1;
	setsockopt(sockfd, IPPROTO_TCP, TCP_NODELAY, (char*)&result, 4);
#endif

	*connection = (struct TIO_CONNECTION*)malloc(sizeof(struct TIO_CONNECTION));
	(*connection)->socket = sockfd;
	(*connection)->serv_addr = serv_addr;
	(*connection)->event_list_queue_end = NULL;
	(*connection)->containers_count = 1;
	(*connection)->containers = malloc(sizeof(void*) * (*connection)->containers_count);
	(*connection)->pending_event_count = 0;
	(*connection)->dispatch_events_on_receive = 1;

	return TIO_SUCCESS;
}

void tio_data_add_to_pr1_message(struct PR1_MESSAGE* pr1_message, unsigned short field_id, const struct TIO_DATA* tio_data)
{
	unsigned short message_field_type = MESSAGE_FIELD_TYPE_NONE;
	const void* buffer = NULL;
	unsigned int data_size = 0;

	if(!tio_data)
		return;

	switch(tio_data->data_type)
	{
	case TIO_DATA_TYPE_NONE:
		message_field_type = MESSAGE_FIELD_TYPE_NONE;
		break;
	case TIO_DATA_TYPE_STRING:
		message_field_type = MESSAGE_FIELD_TYPE_STRING;
		buffer = tio_data->string_;
		data_size = tio_data->string_size_;
		break;
	case TIO_DATA_TYPE_INT:
		message_field_type = MESSAGE_FIELD_TYPE_INT;
		buffer = &tio_data->int_;
		data_size = sizeof(tio_data->int_);
		break;
	case TIO_DATA_TYPE_DOUBLE:
		message_field_type = MESSAGE_FIELD_TYPE_DOUBLE;
		buffer = &tio_data->double_;
		data_size = sizeof(tio_data->double_);
		break;
	};

	pr1_message_add_field(
		pr1_message, 
		field_id, 
		message_field_type,
		buffer, 
		data_size);
}

void pr1_message_field_to_tio_data(const struct PR1_MESSAGE_FIELD_HEADER* field, struct TIO_DATA* tiodata)
{
	if(!tiodata)
		return;

	tiodata_set_as_none(tiodata);

	if(!field)
		return;

	switch(field->data_type)
	{
	case MESSAGE_FIELD_TYPE_NONE:
		break;
	case TIO_DATA_TYPE_STRING:
		tiodata_set_string_and_size(tiodata, &field[1], field->data_size);
		break;
	case TIO_DATA_TYPE_INT:
		tiodata_set_int(tiodata, pr1_message_field_get_int(field));
		break;
	case TIO_DATA_TYPE_DOUBLE:
		tiodata_set_double(tiodata, pr1_message_field_get_double(field));
		break;
	};
}


void pr1_message_field_get_as_tio_data(const struct PR1_MESSAGE* pr1_message, unsigned int field_id, struct TIO_DATA* tiodata)
{
	struct PR1_MESSAGE_FIELD_HEADER* field = NULL;

	tiodata_set_as_none(tiodata);

	field = pr1_message_field_find_by_id(pr1_message, field_id);

	if(!field)
		return;

	pr1_message_field_to_tio_data(field, tiodata);

	return;
}



int tio_container_send_command(struct TIO_CONTAINER* container, unsigned int command_id, const struct TIO_DATA* key, const struct TIO_DATA* value, const struct TIO_DATA* metadata)
{
	SOCKET socket = container->connection->socket;
	int result;

	struct PR1_MESSAGE* pr1_message = pr1_message_new();

	pr1_message_add_field_int(pr1_message, MESSAGE_FIELD_ID_COMMAND, command_id);
	pr1_message_add_field_int(pr1_message, MESSAGE_FIELD_ID_HANDLE, container->handle);

	if(key)
		tio_data_add_to_pr1_message(pr1_message, MESSAGE_FIELD_ID_KEY, key);
	if(value)
		tio_data_add_to_pr1_message(pr1_message, MESSAGE_FIELD_ID_VALUE, value);
	if(metadata)
		tio_data_add_to_pr1_message(pr1_message, MESSAGE_FIELD_ID_METADATA, metadata);

	result = pr1_message_send_and_delete(socket, pr1_message);

	return result;
}


struct EVENT_INFO_NODE
{
	struct EVENT_INFO_NODE* next;
	struct PR1_MESSAGE* message;
};


void events_list_push(struct TIO_CONNECTION* connection, struct PR1_MESSAGE* message)
{
	struct EVENT_INFO_NODE* node = (struct EVENT_INFO_NODE*)malloc(sizeof(struct EVENT_INFO_NODE));

	node->message = message;

	if(connection->event_list_queue_end == NULL)
	{
		node->next = node;
	}
	else
	{
		node->next = connection->event_list_queue_end->next;
		connection->event_list_queue_end->next = node;
	}

	connection->event_list_queue_end = node;
}

struct PR1_MESSAGE* events_list_pop(struct TIO_CONNECTION* connection)
{
	struct EVENT_INFO_NODE* first;
	struct PR1_MESSAGE* pr1_message;

	// empty queue
	if(connection->event_list_queue_end == NULL)
		return NULL;

	first = connection->event_list_queue_end->next;

	// last item?
	if(connection->event_list_queue_end->next == connection->event_list_queue_end)
		connection->event_list_queue_end = NULL;
	else
		connection->event_list_queue_end->next = connection->event_list_queue_end->next->next;

	pr1_message = first->message;

	free(first);

	return pr1_message;
}


int pr1_message_get_error_code(struct PR1_MESSAGE* msg)
{
	struct PR1_MESSAGE_FIELD_HEADER* error_code;

	error_code = pr1_message_field_find_by_id(msg, MESSAGE_FIELD_ID_ERROR_CODE);

	if(!error_code)
		return TIO_SUCCESS;

	return pr1_message_field_get_int(error_code);
}

void tio_disconnect(struct TIO_CONNECTION* connection)
{
	struct PR1_MESSAGE* pending_event;

	if(!connection)
		return;

	closesocket(connection->socket);
	connection->socket = 0;

	while( (pending_event = events_list_pop(connection)) )
		pr1_message_delete(pending_event);

	free(connection->containers);
}


int tio_receive_message(struct TIO_CONNECTION* connection, unsigned int* command_id, struct TIO_DATA* key, struct TIO_DATA* value, struct TIO_DATA* metadata)
{
	int result;
	unsigned int a;
	SOCKET socket = connection->socket;
	const struct PR1_MESSAGE_FIELD_HEADER* current_field;

	struct PR1_MESSAGE* pr1_message;

	result = pr1_message_receive(socket, &pr1_message);
	
	if(TIO_FAILED(result))
		return result;
	
	for(a = 0 ; a < pr1_message->field_count ; a++)
	{
		current_field = pr1_message->field_array[a];
		switch(current_field->field_id)
		{
		case MESSAGE_FIELD_ID_COMMAND:
			*command_id = pr1_message_field_get_int(current_field);
			break;
		case MESSAGE_FIELD_ID_KEY:
			pr1_message_field_to_tio_data(current_field, key);
			break;
		case MESSAGE_FIELD_ID_VALUE:
			pr1_message_field_to_tio_data(current_field, value);
			break;
		case MESSAGE_FIELD_ID_METADATA:
			pr1_message_field_to_tio_data(current_field, metadata);
			break;
		}
	}

	return result;
}

void on_event_receive(struct TIO_CONNECTION* connection, struct PR1_MESSAGE* event_message)
{
	events_list_push(connection, event_message);
	connection->pending_event_count++;

	if(connection->dispatch_events_on_receive)
		tio_dispatch_pending_events(connection, 1);
}

int tio_receive_pending_events(struct TIO_CONNECTION* connection, unsigned int min_events)
{
	int result = 0;
	struct PR1_MESSAGE_FIELD_HEADER* command_field;
	struct PR1_MESSAGE* received_message;

	for(; min_events != 0; min_events--)
	{
		result = pr1_message_receive(connection->socket, &received_message);

		if(TIO_FAILED(result))
			return result;

		command_field = pr1_message_field_find_by_id(received_message, MESSAGE_FIELD_ID_COMMAND);

		if(!command_field) {
			pr1_message_delete(received_message);
			result = TIO_ERROR_PROTOCOL;
			break;
		}

		// MUST be an event
		if(pr1_message_field_get_int(command_field) != TIO_COMMAND_EVENT)
		{
			pr1_message_delete(received_message);
			result = TIO_ERROR_PROTOCOL;
			break;
		}

		on_event_receive(connection, received_message);
	}

	return result;
}

int tio_receive_until_not_event(struct TIO_CONNECTION* connection, struct PR1_MESSAGE** response)
{
	int result;
	struct PR1_MESSAGE_FIELD_HEADER* command_field;
	struct PR1_MESSAGE* received_message;

	// we'll loop until we receive a response (anything that is not an event)
	for(;;)
	{
		result = pr1_message_receive(connection->socket, &received_message);
		if(TIO_FAILED(result))
			return result;

		command_field = pr1_message_field_find_by_id(received_message, MESSAGE_FIELD_ID_COMMAND);

		if(!command_field) {
			*response = NULL;
			pr1_message_delete(received_message);
			*response = NULL;
			return TIO_ERROR_PROTOCOL;
		}

		if(pr1_message_field_get_int(command_field) != TIO_COMMAND_EVENT)
		{
			*response = received_message;
			break;
		}

		on_event_receive(connection, received_message);
	}

	return result;
}

int tio_container_send_command_and_get_response(
	struct TIO_CONTAINER* container, unsigned int command_id, 
	const struct TIO_DATA* key, const struct TIO_DATA* value, const struct TIO_DATA* metadata,
	struct PR1_MESSAGE** response)
{
	int result;
	
	result = tio_container_send_command(container, command_id, key, value, metadata);
	if(TIO_FAILED(result)) 
		return result;

	result = tio_receive_until_not_event(container->connection, response);
	
	if(TIO_FAILED(result)) 
		return result;

	return result;
}


int tio_container_send_command_and_get_data_response(
	struct TIO_CONTAINER* container, unsigned int command_id, 
	const struct TIO_DATA* input_key,
	struct TIO_DATA* key, struct TIO_DATA* value, struct TIO_DATA* metadata)
{
	struct PR1_MESSAGE* response = NULL;
	int result;

	if(key) tiodata_set_as_none(key);
	if(value) tiodata_set_as_none(value);
	if(metadata) tiodata_set_as_none(metadata);

	result = tio_container_send_command(container, command_id, input_key, NULL, NULL);
	if(TIO_FAILED(result)) 
		goto clean_up_and_return;

	result = tio_receive_until_not_event(container->connection, &response);
	if(TIO_FAILED(result)) 
		goto clean_up_and_return;

	result = pr1_message_get_error_code(response);
	if(TIO_FAILED(result)) 
		goto clean_up_and_return;
	
	if(key)
		pr1_message_field_get_as_tio_data(response, MESSAGE_FIELD_ID_KEY, key);

	if(value)
		pr1_message_field_get_as_tio_data(response, MESSAGE_FIELD_ID_VALUE, value);

	if(metadata)
		pr1_message_field_get_as_tio_data(response, MESSAGE_FIELD_ID_METADATA, metadata);

clean_up_and_return:
	pr1_message_delete(response);
	return result;
}




int tio_create_or_open(struct TIO_CONNECTION* connection, unsigned int command_id, const char* name, const char* type, struct TIO_CONTAINER** container)
{
	struct PR1_MESSAGE* pr1_message = NULL;
	struct PR1_MESSAGE* response = NULL;
	struct PR1_MESSAGE_FIELD_HEADER* handle_field = NULL;
	int result;
	int handle;
	SOCKET socket = connection->socket;

	*container = NULL;

	pr1_message = pr1_message_new();

	pr1_message_add_field_int(pr1_message, MESSAGE_FIELD_ID_COMMAND, command_id);
	pr1_message_add_field_string(pr1_message, MESSAGE_FIELD_ID_NAME, name);

	if(type)
		pr1_message_add_field_string(pr1_message, MESSAGE_FIELD_ID_TYPE, type);

	pr1_message_send_and_delete(socket, pr1_message);

	//receive
	result = tio_receive_until_not_event(connection, &response);
	if(TIO_FAILED(result)) 
		goto clean_up_and_return;

	result = pr1_message_get_error_code(response);
	if(TIO_FAILED(result)) 
		goto clean_up_and_return;

	handle_field = pr1_message_field_find_by_id(response, MESSAGE_FIELD_ID_HANDLE);
	if(!handle_field) {
		result = TIO_ERROR_PROTOCOL;
		goto clean_up_and_return;
	}

	*container = (struct TIO_CONTAINER*)malloc(sizeof(struct TIO_CONTAINER));

	handle = pr1_message_field_get_int(handle_field);

	(*container)->connection = connection;
	(*container)->handle = handle;
	(*container)->event_callback = NULL;
	(*container)->cookie = NULL;

	//
	// TODO: I'm indexing by handle because I really don't want to
	// create another hash map implementation now. This array will grow really
	// big if the user open and close lots of containers
	//
	if(handle >= connection->containers_count)
	{
		connection->containers_count = handle * 2;
		connection->containers = realloc(connection->containers, sizeof(void*) * connection->containers_count);
	}
	
	connection->containers[handle] = *container;

clean_up_and_return:
	pr1_message_delete(response);
	return result;
}

int tio_create(struct TIO_CONNECTION* connection, const char* name, const char* type, struct TIO_CONTAINER** container)
{
	return tio_create_or_open(connection, TIO_COMMAND_CREATE, name, type, container);
}

int tio_open(struct TIO_CONNECTION* connection, const char* name, const char* type, struct TIO_CONTAINER** container)
{
	return tio_create_or_open(connection, TIO_COMMAND_OPEN, name, type, container);
}

int tio_close(struct TIO_CONTAINER* container)
{
	struct PR1_MESSAGE* request = NULL;
	struct PR1_MESSAGE* response = NULL;
	int handle;
	int result = TIO_SUCCESS;

	if(!container)
		goto clean_up_and_return;

	handle = container->handle;

	request = pr1_message_new();
	pr1_message_add_field_int(request, MESSAGE_FIELD_ID_COMMAND, TIO_COMMAND_CLOSE);
	pr1_message_add_field_int(request, MESSAGE_FIELD_ID_HANDLE, handle);

	result = pr1_message_send_and_delete(container->connection->socket, request);
	if(TIO_FAILED(result)) 
		goto clean_up_and_return;

	result = tio_receive_until_not_event(container->connection, &response);
	if(TIO_FAILED(result)) 
		goto clean_up_and_return;

	result = pr1_message_get_error_code(response);
	if(TIO_FAILED(result))
		goto clean_up_and_return;

	container->connection->containers[handle] = NULL;

clean_up_and_return:
	pr1_message_delete(response);
	free(container);
	return result;
}

int tio_dispatch_pending_events(struct TIO_CONNECTION* connection, unsigned int max_events)
{
	unsigned int a;
	struct PR1_MESSAGE* event_message;
	struct PR1_MESSAGE_FIELD_HEADER* handle_field;
	struct PR1_MESSAGE_FIELD_HEADER* event_code_field;
	struct TIO_DATA key, value, metadata;
	int handle, event_code;
	void* cookie;
	event_callback_t event_callback;

	tiodata_init(&key);
	tiodata_init(&value);
	tiodata_init(&metadata);

	for(a = 0 ; a < max_events ; a++)
	{
		event_message = events_list_pop(connection);

		if(!event_message)
			break;

		connection->pending_event_count--;

		handle_field = pr1_message_field_find_by_id(event_message, MESSAGE_FIELD_ID_HANDLE);
		event_code_field = pr1_message_field_find_by_id(event_message, MESSAGE_FIELD_ID_EVENT);

		if(handle_field &&
			handle_field->data_type == TIO_DATA_TYPE_INT &&
			event_code_field &&
			event_code_field->data_type == TIO_DATA_TYPE_INT)
		{
			handle = pr1_message_field_get_int(handle_field);
			event_code = pr1_message_field_get_int(event_code_field);

			pr1_message_field_to_tio_data(pr1_message_field_find_by_id(event_message, MESSAGE_FIELD_ID_KEY), &key);
			pr1_message_field_to_tio_data(pr1_message_field_find_by_id(event_message, MESSAGE_FIELD_ID_VALUE), &value);
			pr1_message_field_to_tio_data(pr1_message_field_find_by_id(event_message, MESSAGE_FIELD_ID_METADATA), &metadata);

			event_callback = connection->containers[handle]->event_callback;
			cookie = connection->containers[handle]->cookie;

			if(event_callback)
				event_callback(cookie, handle, event_code, &key, &value, &metadata);
		}

		pr1_message_delete(event_message);
	}

	tiodata_set_as_none(&key);
	tiodata_set_as_none(&value);
	tiodata_set_as_none(&metadata);

	return a;
}


int tio_ping(struct TIO_CONNECTION* connection, char* payload)
{
	struct PR1_MESSAGE* pr1_message = NULL;
	struct PR1_MESSAGE* response = NULL;
	struct PR1_MESSAGE_FIELD_HEADER* value_field = NULL;
	int result;
	unsigned int payload_len;
	SOCKET socket = connection->socket;

	payload_len = strlen(payload);

	pr1_message = pr1_message_new();

	pr1_message_add_field_int(pr1_message, MESSAGE_FIELD_ID_COMMAND, TIO_COMMAND_PING);
	pr1_message_add_field_string(pr1_message, MESSAGE_FIELD_ID_VALUE, payload);

	pr1_message_send_and_delete(socket, pr1_message);

	//receive
	result = tio_receive_until_not_event(connection, &response);
	if(TIO_FAILED(result)) 
		goto clean_up_and_return;

	value_field = pr1_message_field_find_by_id(response, MESSAGE_FIELD_ID_VALUE);

	if(!value_field) {
		result = TIO_ERROR_PROTOCOL;
		goto clean_up_and_return;
	}

	if(payload_len != value_field->data_size)
	{
		result = TIO_ERROR_PROTOCOL;
		goto clean_up_and_return;
	}

	if(memcmp(payload, pr1_message_field_get_buffer(value_field), payload_len) != 0)
	{
		result = TIO_ERROR_PROTOCOL;
		goto clean_up_and_return;
	}

clean_up_and_return:
	pr1_message_delete(response);
	return result;

}

int tio_container_input_command(struct TIO_CONTAINER* container, unsigned short command_id, const struct TIO_DATA* key, const struct TIO_DATA* value, const struct TIO_DATA* metadata)
{
	struct PR1_MESSAGE* response = NULL;
	int result;

	result = tio_container_send_command_and_get_response(container, command_id, key, value, metadata, &response);

	if(TIO_FAILED(result)) 
		goto clean_up_and_return;

	result = pr1_message_get_error_code(response);

	if(TIO_FAILED(result)) 
		goto clean_up_and_return;

	result = TIO_SUCCESS;

clean_up_and_return:
	pr1_message_delete(response);
	return result;
}

int tio_container_propset(struct TIO_CONTAINER* container, const struct TIO_DATA* key, const struct TIO_DATA* value)
{
	return tio_container_input_command(container, TIO_COMMAND_PROPSET, key, value, NULL);
}

int tio_container_push_back(struct TIO_CONTAINER* container, const struct TIO_DATA* key, const struct TIO_DATA* value, const struct TIO_DATA* metadata)
{
	return tio_container_input_command(container, TIO_COMMAND_PUSH_BACK, key, value, metadata);
}

int tio_container_push_front(struct TIO_CONTAINER* container, const struct TIO_DATA* key, const struct TIO_DATA* value, const struct TIO_DATA* metadata)
{
	return tio_container_input_command(container, TIO_COMMAND_PUSH_FRONT, key, value, metadata);
}

int tio_container_set(struct TIO_CONTAINER* container, const struct TIO_DATA* key, const struct TIO_DATA* value, const struct TIO_DATA* metadata)
{
	return tio_container_input_command(container, TIO_COMMAND_SET, key, value, metadata);
}

int tio_container_insert(struct TIO_CONTAINER* container, const struct TIO_DATA* key, const struct TIO_DATA* value, const struct TIO_DATA* metadata)
{
	return tio_container_input_command(container, TIO_COMMAND_INSERT, key, value, metadata);
}

int tio_container_clear(struct TIO_CONTAINER* container)
{
	return tio_container_input_command(container, TIO_COMMAND_CLEAR, NULL, NULL, NULL);
}

int tio_container_delete(struct TIO_CONTAINER* container, const struct TIO_DATA* key)
{
	return tio_container_input_command(container, TIO_COMMAND_DELETE, key, NULL, NULL);
}

int tio_container_pop_back(struct TIO_CONTAINER* container, struct TIO_DATA* key, struct TIO_DATA* value, struct TIO_DATA* metadata)
{
	return tio_container_send_command_and_get_data_response(
		container,
		TIO_COMMAND_POP_BACK,
		NULL,
		key,
		value,
		metadata);
}

int tio_container_pop_front(struct TIO_CONTAINER* container, struct TIO_DATA* key, struct TIO_DATA* value, struct TIO_DATA* metadata)
{
	return tio_container_send_command_and_get_data_response(
		container,
		TIO_COMMAND_POP_FRONT,
		NULL,
		key,
		value,
		metadata);
}

int tio_container_get(struct TIO_CONTAINER* container, const struct TIO_DATA* search_key, struct TIO_DATA* key, struct TIO_DATA* value, struct TIO_DATA* metadata)
{
	return tio_container_send_command_and_get_data_response(
		container,
		TIO_COMMAND_GET,
		search_key,
		key,
		value,
		metadata);
}

int tio_container_propget(struct TIO_CONTAINER* container, const struct TIO_DATA* search_key, struct TIO_DATA* value)
{
	return tio_container_send_command_and_get_data_response(
		container,
		TIO_COMMAND_PROPGET,
		search_key,
		NULL,
		value,
		NULL);
}

int tio_container_get_count(struct TIO_CONTAINER* container, int* count)
{
	int result;
	struct TIO_DATA count_value;

	tiodata_init(&count_value);

	result = tio_container_send_command_and_get_data_response(
		container,
		TIO_COMMAND_COUNT,
		NULL,
		NULL,
		&count_value,
		NULL);

	if(TIO_FAILED(result))
		goto clean_up_and_return;

	assert(count_value.data_type == TIO_DATA_TYPE_INT);

	if(count_value.data_type != TIO_DATA_TYPE_INT)
		return TIO_ERROR_GENERIC;

	*count = count_value.int_;

	result = TIO_SUCCESS;

clean_up_and_return:
	tiodata_set_as_none(&count_value);
	return result;
}


int tio_container_query(struct TIO_CONTAINER* container, int start, int end, query_callback_t query_callback, void* cookie)
{
	int result;
	struct PR1_MESSAGE* request = pr1_message_new();
	struct PR1_MESSAGE* response = NULL;
	struct PR1_MESSAGE* query_item = NULL;
	struct PR1_MESSAGE_FIELD_HEADER* query_id_field = NULL;
	struct PR1_MESSAGE_FIELD_HEADER* command_field = NULL;
	struct TIO_DATA key, value, metadata;
	int query_id;

	pr1_message_add_field_int(request, MESSAGE_FIELD_ID_COMMAND, TIO_COMMAND_QUERY);
	pr1_message_add_field_int(request, MESSAGE_FIELD_ID_HANDLE, container->handle);
	pr1_message_add_field_int(request, MESSAGE_FIELD_ID_START, start);
	pr1_message_add_field_int(request, MESSAGE_FIELD_ID_END, end);


	result = pr1_message_send_and_delete(container->connection->socket, request);
	if(TIO_FAILED(result))
		goto clean_up_and_return;

	result = tio_receive_until_not_event(container->connection, &response);
	if(TIO_FAILED(result))
		goto clean_up_and_return;


	query_id_field = pr1_message_field_find_by_id(response, MESSAGE_FIELD_ID_QUERY_ID);

	if(!query_id_field || query_id_field->data_type != MESSAGE_FIELD_TYPE_INT)
	{
		result = TIO_ERROR_PROTOCOL;
		goto clean_up_and_return;
	}

	query_id = pr1_message_field_get_int(query_id_field);

	tiodata_init(&key); tiodata_init(&value); tiodata_init(&metadata);

	for(;;)
	{
		result = tio_receive_until_not_event(container->connection, &query_item);
		
		if(TIO_FAILED(result))
			goto clean_up_and_return;

		command_field = pr1_message_field_find_by_id(query_item, MESSAGE_FIELD_ID_COMMAND);
		if(!command_field || 
		   command_field->data_type != TIO_DATA_TYPE_INT ||
		   pr1_message_field_get_int(command_field) != TIO_COMMAND_QUERY_ITEM)
		{
			result = TIO_ERROR_PROTOCOL;
			goto clean_up_and_return;
		}

		tiodata_set_as_none(&key); tiodata_set_as_none(&value); tiodata_set_as_none(&metadata);

		pr1_message_field_get_as_tio_data(query_item, MESSAGE_FIELD_ID_KEY, &key);
		pr1_message_field_get_as_tio_data(query_item, MESSAGE_FIELD_ID_VALUE, &value);
		pr1_message_field_get_as_tio_data(query_item, MESSAGE_FIELD_ID_METADATA, &metadata);

		//
		// empty field means query is over
		//
		if(key.data_type == TIO_DATA_TYPE_NONE)
			break;

		query_callback(cookie, query_id, &key, &value, &metadata);
	}


clean_up_and_return:
	tiodata_set_as_none(&key); tiodata_set_as_none(&value); tiodata_set_as_none(&metadata);
	pr1_message_delete(response);
	pr1_message_delete(query_item);

	return result;
}

int tio_container_subscribe(struct TIO_CONTAINER* container, struct TIO_DATA* start, event_callback_t event_callback, void* cookie)
{
	int result;
	
	result = tio_container_input_command(container, TIO_COMMAND_SUBSCRIBE, start, NULL, NULL);
	if(TIO_FAILED(result)) return result;

	container->event_callback = event_callback;
	container->cookie = cookie;

	return TIO_SUCCESS;
}


int tio_container_unsubscribe(struct TIO_CONTAINER* container)
{
	int result;

	container->event_callback = NULL;
	container->cookie = NULL;

	result = tio_container_input_command(container, TIO_COMMAND_UNSUBSCRIBE, NULL, NULL, NULL);
	if(TIO_FAILED(result)) return result;

	return TIO_SUCCESS;
}


void tio_initialize()
{
#ifdef _WIN32
	WSADATA wsadata;
	WSAStartup(MAKEWORD(2,2), &wsadata);
#endif
}



