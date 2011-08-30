#pragma once
/*
	This is the C LANGUAGE Tio client.
	If you're looking for the C++ version, pick the tioclient.hpp header
*/

#ifdef _WIN32
	#define _CRT_SECURE_NO_WARNINGS
	#include <stdio.h>
	#include <tchar.h>
	#include <assert.h>
	#include <WinSock.h>
	#pragma comment(lib ,"ws2_32.lib")
#else
	#include <unistd.h>
	#include <stdlib.h>
	#include <assert.h>
	#include <stdio.h>
	#include <string.h>
	#include <sys/types.h> 
	#include <sys/socket.h>
	#include <netinet/in.h>
	#include <netdb.h>
    #include <errno.h>
	#define closesocket close
#endif

#ifdef __cplusplus
extern "C" {
#endif

#define TIO_DATA_TYPE_NONE	 			0x1
#define TIO_DATA_TYPE_STRING 			0x2
#define TIO_DATA_TYPE_INT 				0x3
#define TIO_DATA_TYPE_DOUBLE 			0x4

#define TIO_SUCCESS						   0
#define TIO_ERROR_GENERIC				  -1
#define TIO_ERROR_NETWORK				  -2
#define TIO_ERROR_PROTOCOL      		  -3
#define TIO_ERROR_MISSING_PARAMETER       -4
#define TIO_ERROR_NO_SUCH_OBJECT	      -5

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


#define TIO_FAILED(x) (x < 0)


#ifndef SOCKET
#define SOCKET int
#endif

struct TIO_DATA
{	
	unsigned int data_type;
	int int_;
	char* string_;
	double double_;
};


typedef void (*event_callback_t)(void* /*cookie*/, unsigned int /*handle*/, unsigned int /*event_code*/, const struct TIO_DATA*, const struct TIO_DATA*, const struct TIO_DATA*);
typedef void (*query_callback_t)(void* /*cookie*/, unsigned int /*queryid*/, const struct TIO_DATA*, const struct TIO_DATA*, const struct TIO_DATA*);

struct TIO_CONNECTION;
struct TIO_CONTAINER;


//
// TIO_DATA related funcions
//
void tiodata_init(struct TIO_DATA* tiodata);
unsigned int tiodata_get_type(struct TIO_DATA* tiodata);
void tiodata_set_as_none(struct TIO_DATA* tiodata);
char* tiodata_set_string_get_buffer(struct TIO_DATA* tiodata, unsigned int min_size);
void tiodata_set_string(struct TIO_DATA* tiodata, const char* value);
void tiodata_set_int(struct TIO_DATA* tiodata, int value);
void tiodata_set_double(struct TIO_DATA* tiodata, double value);

void tiodata_copy(const struct TIO_DATA* source, struct TIO_DATA* destination);
void tiodata_convert_to_string(struct TIO_DATA* tiodata);

//
// tioclient functions
//

// MUST call it before using tio
void tio_initialize();

int tio_connect(const char* host, short port, struct TIO_CONNECTION** connection);
void tio_disconnect(struct TIO_CONNECTION* connection);
int tio_create(struct TIO_CONNECTION* connection, const char* name, const char* type, struct TIO_CONTAINER** container);
int tio_open(struct TIO_CONNECTION* connection, const char* name, const char* type, struct TIO_CONTAINER** container);
int tio_close(struct TIO_CONTAINER* container);

int tio_receive_pending_events(struct TIO_CONNECTION* connection, unsigned int min_events);
int tio_dispatch_pending_events(struct TIO_CONNECTION* connection, unsigned int max_events);


int tio_ping(struct TIO_CONNECTION* connection, char* payload);


int tio_container_propset(struct TIO_CONTAINER* container, const struct TIO_DATA* key, const struct TIO_DATA* value);
int tio_container_propget(struct TIO_CONTAINER* container, const struct TIO_DATA* search_key, struct TIO_DATA* value);

int tio_container_push_back(struct TIO_CONTAINER* container, const struct TIO_DATA* key, const struct TIO_DATA* value, const struct TIO_DATA* metadata);
int tio_container_push_front(struct TIO_CONTAINER* container, const struct TIO_DATA* key, const struct TIO_DATA* value, const struct TIO_DATA* metadata);
int tio_container_pop_back(struct TIO_CONTAINER* container, struct TIO_DATA* key, struct TIO_DATA* value, struct TIO_DATA* metadata);
int tio_container_pop_front(struct TIO_CONTAINER* container, struct TIO_DATA* key, struct TIO_DATA* value, struct TIO_DATA* metadata);
int tio_container_set(struct TIO_CONTAINER* container, const struct TIO_DATA* key, const struct TIO_DATA* value, const struct TIO_DATA* metadata);
int tio_container_insert(struct TIO_CONTAINER* container, const struct TIO_DATA* key, const struct TIO_DATA* value, const struct TIO_DATA* metadata);
int tio_container_clear(struct TIO_CONTAINER* container);
int tio_container_delete(struct TIO_CONTAINER* container, const struct TIO_DATA* key);
int tio_container_get(struct TIO_CONTAINER* container, const struct TIO_DATA* search_key, struct TIO_DATA* key, struct TIO_DATA* value, struct TIO_DATA* metadata);
int tio_container_get_count(struct TIO_CONTAINER* container, int* count);
int tio_container_query(struct TIO_CONTAINER* container, int start, int end, query_callback_t query_callback, void* cookie);
int tio_container_subscribe(struct TIO_CONTAINER* container, struct TIO_DATA* start, event_callback_t event_callback, void* cookie);
int tio_container_unsubscribe(struct TIO_CONTAINER* container);


//
// for plugins
//
struct KEY_AND_VALUE
{
	const char* key;
	const char* value;
};

typedef void (*tio_plugin_start_t)(void* container_manager, struct KEY_AND_VALUE* parameters);
typedef void (*tio_plugin_stop_t)();


#ifdef __cplusplus
} // extern "C" 
#endif
