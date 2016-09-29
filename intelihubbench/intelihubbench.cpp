 // intelihubbench.cpp : Defines the entry point for the console application.
//

#include "stdafx.h"
#include "../InteliHubClient/tioclient.h"
#include "../InteliHubClient/tioclient.hpp"

using std::thread;
using std::function;
using std::vector;
using std::string;
using std::pair;
using std::to_string;
using std::unique_ptr;
using std::make_unique;
using std::unordered_map;

using std::cout;
using std::endl;


class InteliHubTestRunner
{
	vector<function<void(void)>> tests_;
	bool running_;

public:

	InteliHubTestRunner()
		: running_(false)
	{

	}

	void reset()
	{
		tests_.clear();
		running_ = false;
	}

	void add_test(function<void(void)> f)
	{
		tests_.push_back(f);
	}

	void run()
	{
		vector<thread> threads;

		for(auto f : tests_)
		{
			threads.emplace_back(
				thread(
					[f, this]()
					{
						while(!running_)
							std::this_thread::yield();

						f();
					}
			));
		}

		running_ = true;

		for(auto& t : threads)
			t.join();

		reset();
		

	}


};

int vector_perf_test_c(TIO_CONNECTION* cn, TIO_CONTAINER* container, unsigned operations)
{
	int ret;
	TIO_DATA v;

	ret = tio_container_clear(container);
	if(TIO_FAILED(ret)) return ret;

	tiodata_init(&v);
	tiodata_set_string_and_size(&v, "01234567890123456789012345678901", 32);

	tio_begin_network_batch(cn);

	for(unsigned a = 0 ; a < operations ; ++a)
	{
		tio_container_push_back(container, NULL, &v, NULL);
	}

	tio_finish_network_batch(cn);
	tiodata_free(&v);
	return 0;
}


int map_perf_test_c(TIO_CONNECTION* cn, TIO_CONTAINER* container, unsigned operations)
{
	int ret;
	TIO_DATA k, v;

	ret = tio_container_clear(container);
	if(TIO_FAILED(ret)) return ret;

	tiodata_init(&k);
	tiodata_set_string_and_size(&k, "0123456789", 10);

	tiodata_init(&v);
	tiodata_set_string_and_size(&v, "0123456789", 10);

	tio_begin_network_batch(cn);

	for(unsigned a = 0 ; a < operations ; ++a)
	{
		tio_container_set(container, &k, &v, NULL);
	}

	tio_finish_network_batch(cn);

	tiodata_free(&k);
	tiodata_free(&v);

		return 0;
}


typedef int(*PERF_FUNCTION_C)(TIO_CONNECTION*, TIO_CONTAINER *, unsigned int);


int measure(TIO_CONNECTION* cn, TIO_CONTAINER* container, unsigned test_count, 
	PERF_FUNCTION_C perf_function, unsigned* persec)
{
	int ret;

	DWORD start = GetTickCount();

	ret = perf_function(cn, container, test_count);
	if(TIO_FAILED(ret)) return ret;

	DWORD delta = GetTickCount() - start;
	*persec = (test_count * 1000) / delta;

	return ret;
}

class InteliHubTesterSubscriber
{
	vector<pair<string, string>> container_names_;
	string host_name_;
	thread thread_;
	bool should_stop_;

public:

	InteliHubTesterSubscriber(const string& host_name)
		: host_name_(host_name)
		, should_stop_(false)
	{
	}

	InteliHubTesterSubscriber(const InteliHubTesterSubscriber&) = delete;
	InteliHubTesterSubscriber& operator = (const InteliHubTesterSubscriber&) = delete;
	InteliHubTesterSubscriber(InteliHubTesterSubscriber&& rhv) = delete;

	~InteliHubTesterSubscriber()
	{
		assert(!thread_.joinable());
	}

	InteliHubTesterSubscriber(const string& host_name,
		const string& container_name,
		const string& container_type)
		: host_name_(host_name)
		, should_stop_(false)
	{
		add_container(container_name, container_type);
	}


	void add_container(const string& name, const string& type)
	{
		container_names_.push_back(decltype(container_names_)::value_type(name, type));
	}

	void set_containers(decltype(container_names_)& values)
	{
		container_names_ = values;
	}

	void start()
	{
		should_stop_ = false;

		thread_ = std::move(
			thread([&]()
		{
			tio::Connection connection(host_name_);
			vector<tio::containers::list<string>> containers(container_names_.size());

			size_t count = container_names_.size();
			for(size_t i = 0; i < count; i++)
			{
				containers[i].create(
					&connection,
					container_names_[i].first,
					container_names_[i].second);

				containers[i].subscribe(std::bind([](){}));
			}

			while(!should_stop_)
			{
				connection.WaitForNextEventAndDispatch(1);
			}

			connection.Disconnect();

		}));
	}

	void clear()
	{
		thread_.swap(thread());
		container_names_.clear();
	}

	void stop()
	{
		if(!thread_.joinable())
			return;

		should_stop_ = true;
	}

	void join()
	{
		if(!thread_.joinable())
			return;

		should_stop_ = true;

		thread_.join();

	}

};


class InteliHubStressTest
{
	string host_name_;
	string container_name_;
	string container_type_;
	PERF_FUNCTION_C perf_function_;
	unsigned test_count_;
	unsigned* persec_;
public:

	InteliHubStressTest(
		const string& host_name,
		const string& container_name,
		const string& container_type,
		PERF_FUNCTION_C perf_function,
		unsigned test_count,
		unsigned* persec)
		: host_name_(host_name)
		, container_name_(container_name)
		, container_type_(container_type)
		, perf_function_(perf_function)
		, test_count_(test_count)
		, persec_(persec)
	{

	}

	void operator()()
	{
		tio::Connection connection(host_name_);
		tio::containers::list<string> container;
		container.create(&connection, container_name_, container_type_);

		measure(connection.cnptr(), container.handle(), test_count_, perf_function_, persec_);

		container.clear();
	}
};

string generate_container_name()
{
	static unsigned seq = 0;
	static string prefix = "_test_" + to_string(std::chrono::steady_clock::now().time_since_epoch().count());

	return prefix  + "_" + to_string(++seq);
}


int main()
{

#ifdef _DEBUG
	unsigned VOLATILE_TEST_COUNT = 5 * 1000;
	unsigned PERSISTEN_TEST_COUNT = 5 * 1000;
	unsigned MAX_CLIENTS = 1024;
	unsigned MAX_SUBSCRIBERS = 64;

#else
	unsigned VOLATILE_TEST_COUNT = 100 * 1000;
	unsigned PERSISTEN_TEST_COUNT = 10 * 1000;
	unsigned MAX_CLIENTS = 512;
	unsigned MAX_SUBSCRIBERS = 64;
#endif


	const string hostname("localhost");
	InteliHubTestRunner runner;

	int baseline = 0;


	{
		string test_description = "single volatile list, one client";
		unsigned persec;

		runner.add_test(
			InteliHubStressTest(
			hostname,
			generate_container_name(),
			"volatile_list",
			&vector_perf_test_c,
			VOLATILE_TEST_COUNT,
			&persec));

		runner.run();
		
		baseline = persec;
		
		cout << test_description << ": " << persec << " ops/sec" << endl;
	}


	for(unsigned client_count = 1; client_count <= MAX_CLIENTS; client_count *= 2)
	{
		for(unsigned subscriber_count = 1; subscriber_count <= MAX_SUBSCRIBERS; subscriber_count *= 2)
		{
			string test_description = "single volatile list, clients=" + to_string(client_count) + 
				", subscribers=" + to_string(subscriber_count);
			vector<unique_ptr<InteliHubTesterSubscriber>> subscribers;

			vector<unsigned> persec(client_count);
			
			string container_name = generate_container_name();
			string container_type = "volatile_list";

			for(unsigned a = 0; a < client_count; a++)
			{
				runner.add_test(
					InteliHubStressTest(
					hostname,
					container_name,
					container_type,
					&vector_perf_test_c,
					VOLATILE_TEST_COUNT / client_count * 2,
					&persec[a]));
			}

			for(unsigned a = 0; a < subscriber_count; a++)
			{
				subscribers.emplace_back(new InteliHubTesterSubscriber(hostname, container_name, container_type));
					

				(*subscribers.rbegin())->start();
			}

			runner.run();

			for(auto& subscriber : subscribers)
			{
				subscriber->stop();
			}

			for(auto& subscriber : subscribers)
			{
				subscriber->join();
			}

			cout << test_description << ": ";

			int total = 0;

			for(unsigned p : persec)
			{
				cout << p << ", ";
				total += p;
			}

			float vs_baseline = ((float)total / baseline) * 100.0f;
			float slower = 100.0f - vs_baseline;

			cout << "total " << total << " ops/sec"
				 << ", perf vs baseline=" << vs_baseline << "%, " << slower << "% slower" << endl;
		}
	}


	for(int client_count = 1; client_count <= 1024; client_count *= 2)
	{
		string test_description = "multiple volatile lists, client count=" + to_string(client_count);

		vector<unsigned> persec(client_count);

		for(int a = 0; a < client_count; a++)
		{
			runner.add_test(
				InteliHubStressTest(
				"localhost",
				generate_container_name(),
				"volatile_list",
				&vector_perf_test_c,
				VOLATILE_TEST_COUNT / client_count * 2,
				&persec[a]));
		}

		runner.run();

		cout << test_description << ": ";

		unsigned total = 0;

		for(unsigned p : persec)
		{
			cout << p << ", ";
			total += p;
		}

		cout << "total " << total << " ops/sec" << endl;
	}


	//
	// CONNECTIONS TEST
	//
	{
		vector<unique_ptr<tio::Connection>> connections;
		unsigned log_step = 100;

		for(unsigned a = 0; a < MAX_CLIENTS; a++)
		{
			connections.push_back(make_unique<tio::Connection>(hostname));

			if(a % log_step == 0)
				cout << a << "  connections" << endl;
		}
	}


	//{
	//	TIO_CONTAINER* container;
	//	ret = tio_create(cn, "test_volatile_map", "volatile_map", &container);
	//	if(TIO_FAILED(ret)) return ret;

	//	measure(cn, container, VOLATILE_TEST_COUNT, &map_perf_test_c, "volatile map");
	//}


	//{
	//	TIO_CONTAINER* container;
	//	ret = tio_create(cn, "test_persistent_list", "persistent_list", &container);
	//	if(TIO_FAILED(ret)) return ret;

	//	measure(cn, container, PERSISTEN_TEST_COUNT, &vector_perf_test_c, "persistent vector");
	//}

	//{
	//	TIO_CONTAINER* container;
	//	ret = tio_create(cn, "test_persistent_map", "persistent_map", &container);
	//	if(TIO_FAILED(ret)) return ret;

	//	measure(cn, container, PERSISTEN_TEST_COUNT, &map_perf_test_c, "persistent map");
	//}

	//measure(cn, TEST_COUNT, &map_perf_test, "map_perf_test");

	return 0;
}

