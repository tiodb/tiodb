// intelihubbench.cpp : Defines the entry point for the console application.
//

#include "stdafx.h"
#include "../InteliHubClient/tioclient.h"

int vector_perf_test(TIO_CONNECTION* cn, TIO_CONTAINER* container, unsigned operations)
{
	int ret;
	TIO_DATA v;

	ret = tio_container_clear(container);
	if(TIO_FAILED(ret)) return ret;

	tio_begin_network_batch(cn);

	tiodata_init(&v);
	tiodata_set_string_and_size(&v, "01234567890123456789012345678901", 32);

	for(unsigned a = 0 ; a < operations ; ++a)
	{
		tio_container_push_back(container, NULL, &v, NULL);
	}

	tio_finish_network_batch(cn);

	tio_close(container);

	return 0;
}


int map_perf_test(TIO_CONNECTION* cn, TIO_CONTAINER* container, unsigned operations)
{
	int ret;
	TIO_DATA k, v;

	ret = tio_container_clear(container);
	if(TIO_FAILED(ret)) return ret;

	tio_begin_network_batch(cn);

	tiodata_init(&k);
	tiodata_set_string_and_size(&k, "0123456789", 10);

	tiodata_init(&v);
	tiodata_set_string_and_size(&v, "0123456789", 10);

	for(unsigned a = 0 ; a < operations ; ++a)
	{
		tio_container_set(container, &k, &v, NULL);
	}

	tio_finish_network_batch(cn);

	tio_close(container);

	return 0;
}

int measure(TIO_CONNECTION* cn, TIO_CONTAINER* container, unsigned test_count, 
			int (*perf_function)(TIO_CONNECTION*, TIO_CONTAINER *,unsigned int) , const char* desc)
{
	int ret;

	DWORD start = GetTickCount();

	ret = perf_function(cn, container, test_count);
	if(TIO_FAILED(ret)) return ret;

	DWORD delta = GetTickCount() - start;
	DWORD persec = (test_count * 1000) / delta;

	printf("%s: %d op/sec\r\n", desc, persec);

	return ret;
}




int main()
{
	TIO_CONNECTION* cn;
	
	
	int ret;

	ret = tio_connect("localhost", 2605, &cn);
	if(TIO_FAILED(ret)) return ret;

	unsigned VOLATILE_TEST_COUNT = 50 * 1000;
	unsigned PERSISTEN_TEST_COUNT = 1 * 1000;

	{
		TIO_CONTAINER* container;
		ret = tio_create(cn, "test_volatile_list", "volatile_list", &container);
		if(TIO_FAILED(ret)) return ret;

		measure(cn, container, VOLATILE_TEST_COUNT, &vector_perf_test, "volatile vector");
	}


	{
		TIO_CONTAINER* container;
		ret = tio_create(cn, "test_volatile_map", "volatile_map", &container);
		if(TIO_FAILED(ret)) return ret;

		measure(cn, container, VOLATILE_TEST_COUNT, &map_perf_test, "volatile map");
	}


	{
		TIO_CONTAINER* container;
		ret = tio_create(cn, "test_persistent_list", "persistent_list", &container);
		if(TIO_FAILED(ret)) return ret;

		measure(cn, container, PERSISTEN_TEST_COUNT, &vector_perf_test, "persistent vector");
	}

	{
		TIO_CONTAINER* container;
		ret = tio_create(cn, "test_persistent_map", "persistent_map", &container);
		if(TIO_FAILED(ret)) return ret;

		measure(cn, container, PERSISTEN_TEST_COUNT, &map_perf_test, "persistent map");
	}

	//measure(cn, TEST_COUNT, &map_perf_test, "map_perf_test");

	return 0;
}

