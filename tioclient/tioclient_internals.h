#pragma once

#include "tioclient.h"

#ifdef __cplusplus
extern "C" {
#endif

#define MESSAGE_FIELD_TYPE_NONE 		0x1
#define MESSAGE_FIELD_TYPE_STRING 		0x2
#define MESSAGE_FIELD_TYPE_INT	 		0x3
#define MESSAGE_FIELD_TYPE_DOUBLE		0x4

#define MESSAGE_FIELD_ID_COMMAND 		0x1
#define MESSAGE_FIELD_ID_HANDLE 		0x2
#define MESSAGE_FIELD_ID_KEY	 		0x3
#define MESSAGE_FIELD_ID_VALUE			0x4
#define MESSAGE_FIELD_ID_METADATA		0x5
#define MESSAGE_FIELD_ID_NAME			0x6
#define MESSAGE_FIELD_ID_TYPE			0x7
#define MESSAGE_FIELD_ID_ERROR_CODE		0x8
#define MESSAGE_FIELD_ID_ERROR_DESC		0x9
#define MESSAGE_FIELD_ID_EVENT			0xA
#define MESSAGE_FIELD_ID_START_RECORD	0xB
#define MESSAGE_FIELD_ID_END			0xC
#define MESSAGE_FIELD_ID_QUERY_ID		0xD

#define MESSAGE_FIELD_ID_GROUP_NAME		0xE
#define MESSAGE_FIELD_ID_CONTAINER_NAME	0xF
#define MESSAGE_FIELD_ID_CONTAINER_TYPE	0x10

#define MESSAGE_FIELD_ID_QUERY_EXPRESSION 0x11

#define TIO_COMMAND_ANSWER				0x1
#define TIO_COMMAND_EVENT				0x2
#define TIO_COMMAND_QUERY_ITEM			0x3

#define TIO_COMMAND_NEW_GROUP_CONTAINER	0x4


struct PR1_MESSAGE_HEADER
{
	unsigned int message_size;
	unsigned short field_count;
	unsigned short reserved;
};

struct PR1_MESSAGE_FIELD_HEADER
{
	unsigned short field_id;
	unsigned short data_type;
	unsigned int   data_size;
};


struct STREAM_BUFFER
{
	char* buffer;
	char* current;
	unsigned buffer_size;
};

struct PR1_MESSAGE
{
	struct STREAM_BUFFER* stream_buffer;
	struct PR1_MESSAGE_FIELD_HEADER** field_array;
	unsigned short field_count;
};

struct TIO_CONNECTION;

struct TIO_CONTAINER
{
	int handle;
	
	event_callback_t event_callback;
	event_callback_t wait_and_pop_next_callback;
	void* subscription_cookie;

	const char* group_name;
	const char* name;

	void* wait_and_pop_next_cookie;
	struct TIO_CONNECTION* connection;
};

struct TIO_CONNECTION
{
	SOCKET socket;
	char* host;
	unsigned short port;

	struct EVENT_INFO_NODE* event_list_queue_end;
	int pending_event_count;
	int max_pending_event_count;

	unsigned total_messages_received;

	struct TIO_CONTAINER** containers;
	int containers_count;

	event_callback_t group_event_callback;
	void* group_event_cookie;

	int dispatch_events_on_receive;

	unsigned int thread_id;

	int wait_for_answer;
	int pending_answer_count;

	int debug_flags;
};

struct X1_FIELD
{
	char data_type;
	char* value;
};


char* to_lower(char* p);

char* duplicate_string(const char* src);

struct X1_FIELD* x1_decode(const char* buffer, unsigned int size);
void x1_free(struct X1_FIELD* fields);

unsigned int stream_buffer_space_used(struct STREAM_BUFFER* stream_buffer);
unsigned int stream_buffer_space_left(struct STREAM_BUFFER* stream_buffer);


//
// PR1 protocol
//
struct PR1_MESSAGE* pr1_message_new();

void pr1_message_delete(struct PR1_MESSAGE* pr1_message);

void pr1_message_add_field(struct PR1_MESSAGE* pr1_message, unsigned short field_id, unsigned short data_type, const void* buffer, unsigned int buffer_size);
void pr1_message_add_field_int(struct PR1_MESSAGE* pr1_message, unsigned short field_id, int value);
void pr1_message_add_field_double(struct PR1_MESSAGE* pr1_message, unsigned short field_id, double value);
void pr1_message_add_field_string(struct PR1_MESSAGE* pr1_message, unsigned short field_id, const char* value);

int    pr1_message_field_get_int(const struct PR1_MESSAGE_FIELD_HEADER* field);
double pr1_message_field_get_double(const struct PR1_MESSAGE_FIELD_HEADER* field);
void   pr1_message_field_get_string(const struct PR1_MESSAGE_FIELD_HEADER* field, char* buffer, unsigned int buffer_size);


void pr1_message_get_buffer(struct PR1_MESSAGE* pr1_message, void** buffer, unsigned int* size);

void pr1_message_parse(struct PR1_MESSAGE* pr1_message);

struct PR1_MESSAGE_FIELD_HEADER* pr1_message_field_find_by_id(const struct PR1_MESSAGE* pr1_message, unsigned int field_id);

int pr1_message_send(SOCKET socket, struct PR1_MESSAGE* pr1_message);
int pr1_message_send_and_delete(SOCKET socket, struct PR1_MESSAGE* pr1_message);

struct PR1_MESSAGE* pr1_message_new_get_buffer_for_receive(struct PR1_MESSAGE_HEADER* message_header, void** buffer);

int pr1_message_receive(SOCKET socket, struct PR1_MESSAGE** pr1_message,
	const unsigned* message_header_timeout_in_seconds, const unsigned* message_payload_timeout_in_seconds);

//
// TODO: move to internal implementation file
//
struct PR1_MESSAGE* tio_generate_create_or_open_msg(unsigned int command_id, const char* name, const char* type);
struct PR1_MESSAGE* tio_generate_data_message(unsigned int command_id, int handle, const struct TIO_DATA* key, const struct TIO_DATA* value, const struct TIO_DATA* metadata);
int pr1_message_get_error_code(struct PR1_MESSAGE* msg);
void pr1_message_field_to_tio_data(const struct PR1_MESSAGE_FIELD_HEADER* field, struct TIO_DATA* tiodata);
const char* message_field_id_to_string(int i);
const char* tio_command_to_string(int i);
void pr1_message_fill_header_info(struct PR1_MESSAGE* pr1_message);
int pr1_message_get_data_size(struct PR1_MESSAGE* pr1_message);

void dump_pr1_message(const char* prefix, struct PR1_MESSAGE* pr1_message);


void pr1_message_field_get_as_tio_data(const struct PR1_MESSAGE* pr1_message, unsigned int field_id, struct TIO_DATA* tiodata);




#ifdef __cplusplus
} // extern "C" 
#endif
