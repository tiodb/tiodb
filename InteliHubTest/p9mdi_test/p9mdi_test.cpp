// p9mdi_test.cpp : Defines the entry point for the console application.
//

#include "stdafx.h"
#include "..\..\..\intelimarket\p9mdi_client\p9mdi_client.h"
#include <iostream>

using namespace std;


void OnFieldChange(int error_code, void* handle, void* cookie, unsigned eventCode,
	const char* exchange, const char* symbol,
	const char* key, const char* value)
{
	cout << "OnFieldChange: error_code:" << error_code << ", handle:" << handle
		<< ", cookie: " << cookie << endl;
}


void OnListChange(int error_code, void* handle, void* cookie, unsigned eventCode,
	const char* exchange, const char* symbol,
	unsigned position, struct KEY_AND_VALUE* fields)
{
	cout << "OnListChange: error_code:" << error_code << ", handle:" << handle
		<< ", cookie: " << cookie << endl;
}


int _tmain(int argc, _TCHAR* argv[])
{
	P9MDI_CONNECTION* conn = nullptr;
	int result = p9mdi_connect("localhost", 2605, "", "", &conn);

	if (result == 0)
	{
		static char* cookie = nullptr;
		cout << "Connection " << conn << endl;

		result = p9mdi_subscribe_instrument_properties(conn, "bvmf", "petr4", SnapshotPlusIncremental,
			OnFieldChange, ++cookie);

		result = p9mdi_subscribe_instrument_trades(conn, "bvmf", "petr4", 1,
			OnListChange, ++cookie);

		result = p9mdi_subscribe_instrument_order_book(conn, "bvmf", "petr4", 1, 0xFFFFFFFF,
			OnListChange, ++cookie);

		result = p9mdi_subscribe_instrument_order_book(conn, "bvmf", "petr4", 2, 0xFFFFFFFF,
			OnListChange, ++cookie);

		getchar();
		//p9mdi_unsubscribe(conn, handle);
		result = p9mdi_disconnect(conn);
	}

	return 0;
}

