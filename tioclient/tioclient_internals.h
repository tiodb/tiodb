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
#define MESSAGE_FIELD_ID_EVENT			0x9
#define MESSAGE_FIELD_ID_START			0xA
#define MESSAGE_FIELD_ID_END			0xB
#define MESSAGE_FIELD_ID_QUERY_ID		0xC

#define TIO_COMMAND_ANSWER				0x1
#define TIO_COMMAND_EVENT				0x2
#define TIO_COMMAND_QUERY_ITEM			0x3

#define TIO_COMMAND_PING				0x10
#define TIO_COMMAND_OPEN				0x11
#define TIO_COMMAND_CREATE				0x12
#define TIO_COMMAND_CLOSE				0x13
#define TIO_COMMAND_SET					0x14
#define TIO_COMMAND_INSERT				0x15
#define TIO_COMMAND_DELETE				0x16
#define TIO_COMMAND_PUSH_BACK			0x17
#define TIO_COMMAND_PUSH_FRONT			0x18
#define TIO_COMMAND_POP_BACK			0x19
#define TIO_COMMAND_POP_FRONT			0x1A
#define TIO_COMMAND_CLEAR				0x1B
#define TIO_COMMAND_COUNT				0x1C
#define TIO_COMMAND_GET					0x1D
#define TIO_COMMAND_SUBSCRIBE			0x1E
#define TIO_COMMAND_UNSUBSCRIBE			0x1F
#define TIO_COMMAND_QUERY				0x20

#define TIO_COMMAND_PROPGET 			0x30
#define TIO_COMMAND_PROPSET 			0x31


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
	unsigned int buffer_size;
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
	unsigned int handle;
	event_callback_t event_callback;
	void* cookie;
	struct TIO_CONNECTION* connection;
};


struct TIO_CONNECTION
{
	SOCKET socket;
	struct sockaddr_in serv_addr;

	struct EVENT_INFO_NODE* event_list_queue_end;
	int pending_event_count;

	struct TIO_CONTAINER** containers;
	int containers_count;
};

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
void   pr1_message_field_get_string(const struct PR1_MESSAGE_FIELD_HEADER* field, char** buffer, unsigned int buffer_size);

void pr1_message_get_buffer(struct PR1_MESSAGE* pr1_message, void** buffer, unsigned int* size);

void pr1_message_parse(struct PR1_MESSAGE* pr1_message);

struct PR1_MESSAGE_FIELD_HEADER* pr1_message_field_find_by_id(const struct PR1_MESSAGE* pr1_message, unsigned int field_id);

int pr1_message_send(SOCKET socket, struct PR1_MESSAGE* pr1_message);
int pr1_message_send_and_delete(SOCKET socket, struct PR1_MESSAGE* pr1_message);

struct PR1_MESSAGE* pr1_message_new_get_buffer_for_receive(struct PR1_MESSAGE_HEADER* message_header, void** buffer);

int pr1_message_receive(SOCKET socket, struct PR1_MESSAGE** pr1_message);

#ifdef __cplusplus
} // extern "C" 
#endif
