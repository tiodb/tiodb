// InteliHubClientTest.cpp : Defines the entry point for the console application.
//

#include "stdafx.h"
#include "..\InteliHubClient\tioclient_async.hpp"

namespace asio = boost::asio;
using std::string;
using std::cout;
using std::vector;
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





namespace InteliTrader
{
	using namespace tio;
	using namespace tio::containers;
	using std::cout;
	using std::endl;
	using std::shared_ptr;
	using std::function;

	class InteliMarketClient
	{
		AsyncConnection cn_;
		async_map<string, string> securityList_;
		ErrorCallbackT errorCallback_;

		unsigned lastId_;
		typedef async_list<string> TradeOrBookContainerT;

		std::map<unsigned, shared_ptr<TradeOrBookContainerT>> tradeOrBookContainers_;

	public:

		InteliMarketClient()
			: cn_(AsyncConnection::UseOwnThread)
			, lastId_(0)
			, errorCallback_([](const async_error_info& ei){cout << "ERROR: " << ei.error_code << " - " << ei.error_message;})
		{

		}

		void Connect(const string& host, short port)
		{
			cn_.Connect(host, port);
		}

		void SubscribeSecurityList(function<void (const string&)> callback)
		{
			securityList_.open(&cn_, "intelimarket/bvmf/security_list", [](){
				cout << "connected" << endl;
				},
				errorCallback_);
			securityList_.propget("schema", 
				[&](const string& key, const string& value)
				{

				},
				errorCallback_);

			securityList_.subscribe("0", [](){}, errorCallback_, 
				[callback](int code, const string* key, const string* value, const string* metadata)
				{
					if(key)
						callback(*key);
				});
		}

		unsigned SubscribeTrades(const string& exchange, const string& symbol)
		{
			string containerName = "intelimarket/";
			containerName += exchange;
			containerName += "/";
			containerName += symbol;
			containerName += "/trades";

			shared_ptr<TradeOrBookContainerT> container(new TradeOrBookContainerT());
			
			container->open(&cn_, containerName, [](){}, errorCallback_);
			container->subscribe("",
				[symbol](){cout << symbol << " trades subscribed" << endl;}, 
				errorCallback_,
				[symbol](int code, const int* key, const string* value, const string* metadata)
				{
					if(value)
						cout << "TRADE " << symbol << ":" << *value << endl;
				});

			unsigned id = ++lastId_;

			tradeOrBookContainers_[id] = container;

			return id;
		}

		unsigned SubscribeBookBuy(const string& exchange, const string& symbol)
		{
			string containerName = "intelimarket/";
			containerName += exchange;
			containerName += "/";
			containerName += symbol;
			containerName += "/book_buy";

			shared_ptr<TradeOrBookContainerT> container(new TradeOrBookContainerT());

			container->open(&cn_, containerName, [](){cout << "opened" << endl;}, errorCallback_);
			container->subscribe("0",
				[symbol](){cout << symbol << " book buy subscribed" << endl;}, 
				errorCallback_,
				[symbol](int code, const int* key, const string* value, const string* metadata)
				{
					if(value)
						cout << "BOOK  " << symbol << ":" << *value << endl;
				});

			unsigned id = ++lastId_;

			tradeOrBookContainers_[id] = container;

			return id;
		}

		unsigned SubscribeBookSell(const string& exchange, const string& symbol)
		{
			string containerName = "intelimarket/";
			containerName += exchange;
			containerName += "/";
			containerName += symbol;
			containerName += "/book_sell";

			shared_ptr<TradeOrBookContainerT> container(new TradeOrBookContainerT());

			container->open(&cn_, containerName, [](){}, errorCallback_);
			container->subscribe("0",
				[symbol](){cout << symbol << " book sell subscribed" << endl;}, 
				errorCallback_,
				[symbol](int code, const int* key, const string* value, const string* metadata)
			{
				if(value)
					cout << "BOOK  " << symbol << ":" << *value << endl;
			});

			unsigned id = ++lastId_;

			tradeOrBookContainers_[id] = container;

			return id;
		}
	};
}


void TestInteliMarketClient()
{
	using std::vector;
	using std::string;


	InteliTrader::InteliMarketClient client;

	client.Connect("10.255.232.50", 2605);

	//client.SubscribeSecurityList();

	vector<string> symbols;

	int max = 0;

	client.SubscribeSecurityList(
		[&](const string& symbol)
		{
			if(max == 0)
				return;

			max--;

			string s = boost::algorithm::to_lower_copy(symbol);
			cout << "subscribing " << s << endl;
			client.SubscribeBookBuy("bvmf", s);
		});

	if(1)
	{
		symbols.push_back("petr4");
		symbols.push_back("vale5");
		symbols.push_back("ccro3");
		symbols.push_back("vale3");
		symbols.push_back("bbdc3");
		symbols.push_back("ggbr4");
		symbols.push_back("ogxp3");
		symbols.push_back("sanb11");
		symbols.push_back("netc3");
		symbols.push_back("netc4");
	}

	for each(const string& s in symbols)
	{
		client.SubscribeTrades("bvmf", s);
		client.SubscribeBookBuy("bvmf", s);
	}

	for(bool b = true ; b ; )
		Sleep(50);
}
void group_test_callback(const char* group_name, const char* container_name, 
	unsigned int event_code, const struct TIO_DATA* k, const struct TIO_DATA* v, const struct TIO_DATA* m)
{
	cout << group_name << ", " << container_name << ", " << event_code << endl;
	return;
}


void test_group_subscribe()
{
	TIO_CONNECTION* cn;
	static const int CONTAINER_COUNT = 5;
	static const int ITEM_COUNT_BEFORE = 5;
	const char* group_name = "test_group";
	vector<TIO_CONTAINER*> containers;

	tio_connect("localhost", 2605, &cn);

	cout << "creating and filling containers..." << endl;
	
	for(int a = 0 ; a < CONTAINER_COUNT ; a++)
	{
		string name = "container_";
		name += lexical_cast<string>(a);

		TIO_CONTAINER* container;

		tio_create(cn, name.c_str(), "volatile_list", &container);
		containers.push_back(container);

		for(int b = 0 ; b < ITEM_COUNT_BEFORE ; b++)
		{
			TIO_DATA value;

			tiodata_init(&value);
			tiodata_set_int(&value, b);

			tio_container_push_back(containers[a], NULL, &value, NULL);
		}
	}

	for(int a = 0 ; a < CONTAINER_COUNT ; a++)
	{
		tio_group_add(cn, group_name, containers[a]->name);
	}

	cout << "subscribing..." << endl;

	tio_group_set_subscription_callback(cn, &group_test_callback);
	tio_group_subscribe(cn, group_name, "0");

	tio_dispatch_pending_events(cn, 0xFFFFFFFF);

	cout << "adding more records..." << endl;

	for(int a = 0 ; a < CONTAINER_COUNT ; a++)
	{
		for(int b = 0 ; b < ITEM_COUNT_BEFORE ; b++)
		{
			TIO_DATA value;

			tiodata_init(&value);
			tiodata_set_int(&value, b);

			tio_container_push_back(containers[a], NULL, &value, NULL);
		}
	}

	tio_dispatch_pending_events(cn, 0xFFFFFFFF);
}

int _tmain(int argc, _TCHAR* argv[])
{
	test_group_subscribe();

	return 0;

	/*
	TestInteliMarketClient();
	
	AsyncClientTest();
	QueueModificationsWhileConnecting();
	AsyncClientTest();
	return 0;
	*/
}
