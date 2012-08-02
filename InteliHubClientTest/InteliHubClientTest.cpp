// InteliHubClientTest.cpp : Defines the entry point for the console application.
//

#include "stdafx.h"
#include "..\InteliHubClient\tioclient_async.hpp"

namespace asio = boost::asio;
using std::string;
using std::cout;
using std::endl;
using tio::async_error_info;
using boost::lexical_cast;


struct WnpNextHandler
{
	tio::containers::async_list<string>* wnp_list_;

	WnpNextHandler() : wnp_list_(nullptr){}

	void Start(tio::containers::async_list<string>* wnp_list)
	{
		wnp_list_ = wnp_list;
		Beg4Moar();
	}

	void Beg4Moar()
	{
		wnp_list_->wait_and_pop_next(
			[](){},
			[](const async_error_info& error)
			{
				cout << "ERROR: " << endl << endl;
			},
			[this](int eventCode, const int* key, const string* value, const string* metadata)
			{
				OnData(eventCode, key, value, metadata);
				Beg4Moar();
			});
	}

	void OnData(int eventCode, const int* key, const string* value, const string* metadata)
	{
		cout << "wnp_next " << eventCode 
			<< ", key: " << *key
			<< ", value: " << (value ? *value : "(null)")
			<< ", metadata: " << (metadata ? *metadata : "(null)")
			<< endl;
	}
};

void QueueModificationsWhileConnecting()
{
	asio::io_service io_service;

	tio::AsyncConnection cm(io_service);
	cm.Connect("localhost", 2605);

	auto errorHandler = [](const async_error_info& error)
	{
		cout << "ERROR: " << endl << endl;
	};

	tio::containers::async_map<string, string> m;

	m.create(&cm, "am", "volatile_map", NULL, errorHandler);

	m.set("abc", "abc", nullptr, [](){}, errorHandler);

	m.subscribe("0",
		[](){},
		errorHandler,
		[](int code, const string* k, const string* v, const string* m)
		{
			if(code == TIO_EVENT_SNAPSHOT_END)
				cout << "snapshot end" << endl;
			else
				cout << code << " - k=" << *k << ", v=" << *v << endl;
		});

	for(int a = 0 ; a < 10 ; a++)
	{
		string key = lexical_cast<string>(a);
		cout << "setting \"" << key << "\"" << endl;
		m.set(key, lexical_cast<string>(a), NULL, [key](){cout << "\"" << key << "\" set" << endl; }, errorHandler);
	}

	io_service.run();
}


void AsyncClientTest()
{
	asio::io_service io_service;

	//tio::AsyncConnection cm(io_service);
	tio::AsyncConnection cm(tio::AsyncConnection::UseOwnThread);
	cm.Connect("localhost", 2605);

	auto errorHandler = [](const async_error_info& error)
	{
		cout << "ERROR: " << endl << endl;
	};

	tio::containers::async_map<string, string> m, m2, m3;

	auto do_a_lot_of_stuff = 
		[&]()
		{
			m.propset("a", "10",
				[]() { cout << "propset done" << endl; },
				errorHandler);

			m.propget("a",
				[](const string& key, const string& value) 
				{ 
					cout << "propget key=" << key << ", value=" << value << endl; 
				},
				errorHandler);

			m.subscribe("",
				[]()
				{
					cout << "subscribed" << endl;
				},
				errorHandler,
				[](int eventCode, const string* key, const string* value, const string* metadata)
				{
					cout << "event " << eventCode 
						<< ", key: " << (key ? *key : "(null)")
						<< ", value: " << (value ? *value : "(null)")
						<< ", metadata: " << (metadata ? *metadata : "(null)")
						<< endl;
				}
			);

			for(int a = 0 ; a < 10 ; a++)
			{
				m.set("a", "b", nullptr,
					[]()
					{
						cout << "set command done" << endl;
					},
					errorHandler);

				m.get("a", 
					[](const string* key, const string* value, const string* metadata)
					{
						cout << "get: " << *key << "=" << *value << endl;
					},
					errorHandler);

				m.set(lexical_cast<string>(a), lexical_cast<string>(a), NULL, NULL, errorHandler);
			}
		};

	m.create(&cm, "am", "volatile_map", do_a_lot_of_stuff, errorHandler);

	m3.open(&cm, "am", 
		[&]()
		{
			m3.query(nullptr, nullptr,[](){}, errorHandler,
				[](int eventCode, const string* key, const string* value, const string* metadata)
				{
					cout << "query " << eventCode 
						<< ", key: " << (key ? *key : "(null)")
						<< ", value: " << (value ? *value : "(null)")
						<< ", metadata: " << (metadata ? *metadata : "(null)")
						<< endl;
				}
			);
		}
		, errorHandler);


	tio::containers::async_list<string> l1;
	WnpNextHandler wnpHandler;
	
	l1.create(&cm, "l1", "volatile_list",
		[&]()
		{
			l1.push_back("1", nullptr, NULL, NULL);
			l1.push_back("2", nullptr, NULL, NULL);
			l1.push_back("3", nullptr, NULL, NULL);
			l1.push_back("4", nullptr, NULL, NULL);
			l1.push_back("5", nullptr, NULL, NULL);
			l1.push_back("6", nullptr, NULL, NULL);

			wnpHandler.Start(&l1);
		},
		errorHandler
	);

	if(cm.usingSeparatedThread())
	{
		io_service.post([]()
		{
			for(bool b = true ; b ; )
				Sleep(100);
		});
	}

	io_service.run();

	cm.Disconnect();

	return;
}


int _tmain(int argc, _TCHAR* argv[])
{
	AsyncClientTest();
	QueueModificationsWhileConnecting();
	AsyncClientTest();
	return 0;
}
