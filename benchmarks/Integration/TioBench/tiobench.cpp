
#include "tioclient.h"
#include "tioclient.hpp"
#include "tioclient_internals.h"
#include <atomic>
#include <chrono>
#include <functional>
#include <iostream> // std::cout
#include <map>
#include <sstream> // std::stringstream
#include <thread>
#include <unordered_map>
#include <numeric>
#include <vector>

using std::accumulate;
using std::atomic;
using std::function;
using std::make_unique;
using std::map;
using std::pair;
using std::string;
using std::thread;
using std::to_string;
using std::unique_ptr;
using std::unordered_map;
using std::vector;

using std::cout;
using std::endl;

using std::chrono::duration_cast;
using std::chrono::high_resolution_clock;
using std::chrono::milliseconds;

using chrono_tp = std::chrono::high_resolution_clock::time_point;

static int assertiveEventCount = 0;

class TioTestRunner
{
	vector<function<void(void)>> tests_;
	bool running_;

	vector<thread> threads_;
	atomic<unsigned> finishedThreads_;

public:

	TioTestRunner()
		: running_(false)
	{
		reset();
	}

	void reset()
	{
		tests_.clear();
		running_ = false;
		finishedThreads_ = 0;
	}

	void add_test(function<void(void)> f)
	{
		tests_.push_back(f);
	}

	void join()
	{
		for (auto& t : threads_)
			t.join();

		reset();
	}

	void run()
	{
		start();
		join();
	}

	bool finished() const
	{
		return finishedThreads_ == threads_.size();
	}

	void start()
	{
		for (auto f : tests_)
		{
			threads_.emplace_back(
				thread(
					[f, this]()
			{
				while (!running_)
					std::this_thread::yield();

				try
				{
					f();
				}
				catch (std::exception& ex)
				{
					//__debugbreak();

				}

				finishedThreads_++;
			}
			));
		}

		running_ = true;
	}
};

string get_seq_value(string container_name, int operation)
{
	std::stringstream ss;
	ss << container_name << ";" << operation << ";" << "_01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901";
	string val = ss.str();
	val = val.substr(0, 48);
	//snprintf(value, bufferCount, "(%s)(%i)_01234567890123456789012345678901234567890123456789012345678901", container_name, operation);
	return val;
}

int vector_perf_test_c(TIO_CONNECTION* cn, TIO_CONTAINER* container, unsigned operations)
{
	TIO_DATA v;

	tiodata_init(&v);

	tio_begin_network_batch(cn);

	for (unsigned a = 0; a < operations; ++a)
	{
		string val = get_seq_value(container->name, a);

		tiodata_set_string_and_size(&v, val.c_str(), val.size());

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

	tiodata_init(&k);
	tiodata_init(&v);

	tio_begin_network_batch(cn);

	string prefix = to_string(reinterpret_cast<uint64_t>(cn)) + "_";

	for (unsigned a = 0; a < operations; ++a)
	{
		char buffer[16];
        sprintf(buffer, "%d", a);
		string keyAsString = prefix + buffer;
		tiodata_set_string_and_size(&k, keyAsString.c_str(), keyAsString.size());

		tiodata_set_int(&v, a);

		tio_container_set(container, &k, &v, NULL);
	}

	tio_finish_network_batch(cn);

	tiodata_free(&k);
	tiodata_free(&v);

	return 0;
}


typedef int(*PERF_FUNCTION_C)(TIO_CONNECTION*, TIO_CONTAINER *, unsigned int);


int measure(TIO_CONNECTION* cn, TIO_CONTAINER* container, unsigned test_count,
	PERF_FUNCTION_C perf_function, unsigned* persec, const std::string& container_name, 
	chrono_tp* start_time = nullptr, chrono_tp* end_time = nullptr)
{
	int ret;
	
	auto start = std::chrono::high_resolution_clock::now();
	if (start_time != nullptr) {
		*start_time = start;
	}
    
	ret = perf_function(cn, container, test_count);
	if (TIO_FAILED(ret)) return ret;

	auto end = std::chrono::high_resolution_clock::now();
	auto delta = end - start;
	auto time_elpased_in_milliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(delta).count();
	*persec = (test_count * 1000) / time_elpased_in_milliseconds;

	if (end_time != nullptr) {
		*end_time = end;
	}

	return ret;
}

struct ContainerData
{
	string name;
	string type;
	int index = 0;
	ContainerData(string p_name, string p_type)
	{
		name = p_name;
		type = p_type;
	}
};
class TioTesterSubscriber
{
	vector<ContainerData> container_names_;
	string host_name_;
	thread thread_;
	bool should_stop_;
	uint64_t eventCount_;
	bool run_sequence_test = false;
	map<string, chrono_tp> containers_start_times_;
	map<string, size_t> deltas_;
public:
	TioTesterSubscriber(const string& host_name)
		: host_name_(host_name)
		, should_stop_(false)
		, eventCount_(0)
	{
	}

	TioTesterSubscriber(const string& host_name,
		const string& container_name,
		const string& container_type)
		: host_name_(host_name)
		, should_stop_(false)
		, eventCount_(0)
	{
		add_container(container_name, container_type);
	}

	TioTesterSubscriber(const TioTesterSubscriber&) = delete;
	TioTesterSubscriber& operator = (const TioTesterSubscriber&) = delete;
	TioTesterSubscriber(TioTesterSubscriber&& rhv) = delete;

	~TioTesterSubscriber()
	{
		assert(!thread_.joinable());
	}

	map<string, size_t> deltas() const { return deltas_; }

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

			for (size_t i = 0; i < count; i++)
			{
				containers[i].create(
					&connection,
					container_names_[i].name,
					container_names_[i].type);
				
				int startPosition = 0;

				auto eventHandler = [this](const string& containerName, const string& eventName, const int& key, const string& value)
				{
					++this->eventCount_;
					if (run_sequence_test)
					{
						bool notFindContainer = true;
						for (int j = 0; j < container_names_.size(); j++)
						{
							size_t name_position = value.find(containerName);
							if (name_position != string::npos)
							{
								assertiveEventCount++;
								notFindContainer = false;
								string expectedValue = get_seq_value(container_names_[j].name, container_names_[j].index++);
								if (value.compare(expectedValue) != 0)
								{
									run_sequence_test = false;
									printf("\r\nSequence error:Expected \"%s\"", expectedValue.c_str());
									printf("\r\nbut found              \"%s\"", value.c_str());
									printf("\r\nCorrect sequence found until:%i", assertiveEventCount);
									break;
								}
							}
							if (notFindContainer)
							{
								run_sequence_test = false;
								printf("\r\nNot found container information in \"%s\"!", value.c_str());
								printf("\r\n\"Correct sequece found:%i\"", assertiveEventCount);
								break;
							}
						}
					}
				};

				containers_start_times_.insert({ container_names_[i].name, std::chrono::high_resolution_clock::now() });
				containers[i].subscribe(eventHandler, &startPosition);
			}

			while (!should_stop_)
			{
				connection.WaitForNextEventAndDispatch(1);
			}

			for (auto& c : container_names_)
			{
				auto timer_end = std::chrono::high_resolution_clock::now();
				auto timer_start = containers_start_times_[c.name];

				auto delta =
					std::chrono::duration_cast<std::chrono::milliseconds>(timer_end - timer_start).count();

				deltas_.insert({ c.name, delta });
			}

			connection.Disconnect();

		}));
	}

	void clear()
	{
        thread t;
		thread_.swap(t);
		container_names_.clear();
	}

	void stop()
	{
		if (!thread_.joinable())
			return;

		should_stop_ = true;
	}

	void join()
	{
		if (!thread_.joinable())
			return;

		should_stop_ = true;

		thread_.join();

	}

};


class TioStressTest
{
	string host_name_;
	string container_name_;
	string container_type_;
	PERF_FUNCTION_C perf_function_;
	unsigned test_count_;
	unsigned* persec_;

	chrono_tp* start_time_;
	chrono_tp* end_time_;
public:

	TioStressTest(
		const string& host_name,
		const string& container_name,
		const string& container_type,
		PERF_FUNCTION_C perf_function,
		unsigned test_count,
		unsigned* persec,
		chrono_tp* start_time = nullptr,
		chrono_tp* end_time = nullptr)
		: host_name_(host_name)
		, container_name_(container_name)
		, container_type_(container_type)
		, perf_function_(perf_function)
		, test_count_(test_count)
		, persec_(persec)
		, start_time_(start_time)
		, end_time_(end_time)
	{

	}

	void operator()()
	{
		tio::Connection connection(host_name_);
		tio::containers::list<string> container;
		container.create(&connection, container_name_, container_type_);

		measure(connection.cnptr(), container.handle(), test_count_, perf_function_, persec_, container_name_, start_time_, end_time_);

		container.clear();
	}
};

string generate_container_name()
{
	static unsigned seq = 0;
	static string prefix = "_test_" + to_string(std::chrono::steady_clock::now().time_since_epoch().count());

	return prefix + "_" + to_string(++seq) + "_";
}

void TEST_deadlock_on_disconnect(const char* hostname)
{
	static const unsigned PUBLISHER_COUNT = 60;
	static const unsigned ITEM_COUNT = 2 * 1000;

	const string container_type("volatile_list");

	cout << "START: deadlock test" << endl;

	tio::Connection subscriberConnection(hostname);

	vector<tio::containers::list<string>> containers(PUBLISHER_COUNT);
	vector<unsigned> persec(PUBLISHER_COUNT);

	for (unsigned a = 0; a < PUBLISHER_COUNT; a++)
	{
		containers[a].create(&subscriberConnection, generate_container_name(), container_type);
		containers[a].subscribe(std::bind([]() {}));
	}

	TioTestRunner runner;

	for (unsigned a = 0; a < PUBLISHER_COUNT; a++)
	{
		runner.add_test(
			TioStressTest(
				hostname,
				containers[a].name(),
				container_type,
				&vector_perf_test_c,
				ITEM_COUNT,
				&persec[a]));
	}

	runner.start();

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

	subscriberConnection.Disconnect();

	unsigned publicationCount = 0;

	for (int a = 0;; a++)
	{
		if (runner.finished())
			break;


		cout << "creating new subscribers... ";

		tio::Connection newConnection(hostname);
		vector<tio::containers::list<string>> newContainers(PUBLISHER_COUNT);

		for (unsigned a = 0; a < PUBLISHER_COUNT; a++)
		{
			newContainers[a].open(&newConnection, containers[a].name());
			newContainers[a].subscribe(std::bind([&]() { publicationCount++; }));
		}

		cout << "done." << endl;

		for (int b = 0; b < 1; b++)
			newConnection.WaitForNextEventAndDispatch(1);

		newConnection.Disconnect();

		cout << "not yet. Publication count = " << publicationCount << (a > 10 ? " PROBABLY DEADLOCK" : "") << endl;

        std::this_thread::sleep_for(std::chrono::milliseconds(500));
	}

	cout << "no deadlock" << endl;

	runner.join();
}


void TEST_connection_stress(
	const char* hostname,
	unsigned connection_count)
{
	cout << "START: connection stress test, " << connection_count << " connections" << endl;
	vector<unique_ptr<tio::Connection>> connections;
	connections.reserve(connection_count);
	unsigned log_step = 1000;

    auto start = std::chrono::high_resolution_clock::now();

	for (unsigned a = 0; a < connection_count; a++)
	{
		connections.push_back(make_unique<tio::Connection>(hostname));

		if (a % log_step == 0)
			cout << a << "  connections" << endl;
	}

	cout << "disconnecting..." << endl;

	connections.clear();

	auto delta = std::chrono::high_resolution_clock::now() - start;

	cout << "FINISHED: connection stress test, " << std::chrono::duration_cast<std::chrono::milliseconds>(delta).count() << "ms" << endl;
}


void TEST_container_concurrency(
	const char* hostname,
	unsigned max_client_count,
	unsigned item_count,
	const char* container_type,
	PERF_FUNCTION_C perf_function
)
{
	cout << "START: container concurrency test, type=" << container_type << endl;

	for (unsigned client_count = 1; client_count <= max_client_count; client_count *= 2)
	{
		TioTestRunner testRunner;

		unsigned items_per_thread = item_count / client_count;

		cout << client_count << " clients, " << items_per_thread << " items per thread... ";

        auto start = high_resolution_clock::now();

		string container_name = generate_container_name();

		for (unsigned a = 0; a < client_count; a++)
		{
			testRunner.add_test(
				[=]()
			{
				tio::Connection connection(hostname);
				tio::containers::list<string> c;

				c.create(&connection, container_name, container_type);

				perf_function(connection.cnptr(), c.handle(), items_per_thread);
			}
			);
		}

		testRunner.run();

		auto delta_ms = duration_cast<milliseconds>(high_resolution_clock::now() - start).count();
		auto persec = item_count / delta_ms;

		tio::Connection connection(hostname);
		tio::containers::list<string> c;

		c.open(&connection, container_name);

		const char* errorMessage = (c.size() != (items_per_thread * client_count)) ? " LOST RECORDS!" : "";

		cout << delta_ms << "ms (" << "items_per_second= " << persec << "k/s)" << errorMessage << endl;
	}

	cout << "FINISH: container concurrency test" << endl;
}


void TEST_create_lots_of_containers(const char* hostname,
	unsigned container_count,
	unsigned client_count,
	unsigned item_count)
{
	tio::Connection connection(hostname);
	vector<tio::containers::list<string>> containers(container_count);

	TioTestRunner testRunner;

	unsigned containers_per_thread = container_count / client_count;

	cout << "START: container stress test, " << container_count << " containers, "
		<< client_count << " clients" << endl;

	auto start = high_resolution_clock::now();

	for (unsigned a = 0; a < client_count; a++)
	{
		testRunner.add_test(
			[hostname, prefix = "l_" + to_string(a), container_count, item_count]()
		{
			tio::Connection connection(hostname);
			vector<tio::containers::list<string>> containers(container_count);

			for (unsigned a = 0; a < container_count; a++)
			{
				auto& c = containers[a];
				string name = prefix + to_string(a);
				c.create(&connection, name, "volatile_list");
			}

			for (unsigned a = 0; a < container_count; a++)
			{
				auto& c = containers[a];
				vector_perf_test_c(connection.cnptr(), c.handle(), item_count);
			}

			containers.clear();
		}
		);
	}

	testRunner.run();

	auto delta = duration_cast<milliseconds>(high_resolution_clock::now() - start).count();

	cout << "FINISH: container stress test, " << delta << "ms" << endl;

	containers.clear();
}

struct BenchTimer {
	chrono_tp start_time;
	chrono_tp end_time;
};

void TEST_data_stress_test(const char* hostname,
	unsigned max_client_count,
	unsigned max_subscribers,
	unsigned item_count)
{	
	cout << "==================================" << " BEGIN TEST " << "==================================\n";
	cout << "data stress test, "
		<< "MAX_CLIENTS=" << max_client_count
		<< ", MAX_SUBSCRIBERS=" << max_subscribers
		<< ", ITEM_COUNT=" << item_count
		<< "\n";

	int baseline = 0;

	{
		TioTestRunner runner;

		string test_description = "single volatile list, one client";
		unsigned persec;

		BenchTimer bench_timer;

		runner.add_test(
			TioStressTest(
				hostname,
				generate_container_name(),
				"volatile_list",
				&vector_perf_test_c,
				item_count,
				&persec,
				&bench_timer.start_time,
				&bench_timer.end_time));

		runner.run();

		baseline = persec;

		cout << test_description << ": " << persec << " ops/sec (baseline)" << "\n";
	}

	for (unsigned client_count = 1; client_count <= max_client_count; client_count *= 2)
	{
		for (unsigned subscriber_count = 0; subscriber_count <= max_subscribers; subscriber_count *= 2)
		{
			TioTestRunner runner;

			vector<unique_ptr<TioTesterSubscriber>> subscribers;

			vector<unsigned> persec(client_count);
			vector<BenchTimer> bench_timers(client_count);

			string container_name = generate_container_name();
			string container_type = "volatile_list";

			for (unsigned a = 0; a < client_count; a++)
			{
				runner.add_test(
					TioStressTest(
						hostname,
						container_name,
						container_type,
						&vector_perf_test_c,
						item_count,
						&persec[a],
						&bench_timers[a].start_time,
						&bench_timers[a].end_time));
			}

			for (unsigned a = 0; a < subscriber_count; a++)
			{
				subscribers.emplace_back(new TioTesterSubscriber(hostname, container_name, container_type));
				(*subscribers.rbegin())->start();
			}

			runner.run();

			for (auto& subscriber : subscribers)
			{
				subscriber->stop();
			}

			for (auto& subscriber : subscribers)
			{
				subscriber->join();
			}

			int inserts_ms{};
			for (const auto& inserts : bench_timers) {
				const auto delta = inserts.end_time - inserts.start_time;
				inserts_ms += std::chrono::duration_cast<std::chrono::milliseconds>(delta).count();
			}
			auto inserts_average = inserts_ms / bench_timers.size();

			int callbacks_ms{};
			for (const auto& s : subscribers) {
				const auto ms = s->deltas()[container_name];

				callbacks_ms += ms;
			}
			int callbacks_average{};
			if(subscribers.size() > 0)
				callbacks_average= callbacks_ms / subscribers.size();

			cout 
				<< "single volatile list " << "\n" 
				<< "Clients: " << client_count << "\n"
				<< "Subscribers: " << subscriber_count << "\n";
				
			int total = accumulate(cbegin(persec), cend(persec), 0);

			float vs_baseline = ((float)total / baseline) * 100.0f;
			
			cout
				<< "Average of inserts: " << inserts_average << " ms" << "\n"
				<< "Average of callbacks: " << callbacks_average << " ms" << "\n";

			cout 
				<< "Total: " << total << " ops/sec" << "\n"
				<< "perf vs baseline: " << vs_baseline << "% - ";

			for (unsigned p : persec)
				cout << p << ",";

			cout << "\n----------------------------------------\n";

			if (subscriber_count == 0)
				subscriber_count = 1;
		}
	}


	for (int client_count = 1; client_count <= 1024; client_count *= 2)
	{
		TioTestRunner runner;

		string test_description = "multiple volatile lists, client count=" + to_string(client_count);

		vector<unsigned> persec(client_count);

		for (int a = 0; a < client_count; a++)
		{
			runner.add_test(
				TioStressTest(
					hostname,
					generate_container_name(),
					"volatile_list",
					&vector_perf_test_c,
					item_count / client_count * 2,
					&persec[a]));
		}

		runner.run();

		cout << test_description << ": ";

		unsigned total = 0;

		for (unsigned p : persec)
		{
			cout << p << ", ";
			total += p;
		}

		cout << "total " << total << " ops/sec" << endl;
	}

	cout << "==================================" << " END TEST " << "==================================\n";
}


int main()
{

#ifdef _DEBUG
	unsigned VOLATILE_TEST_COUNT = 1 * 1000;
	unsigned PERSISTEN_TEST_COUNT = 1 * 1000;
	unsigned MAX_CLIENTS = 8;
	unsigned CONNECTION_STRESS_TEST_COUNT = 10 * 1000;
	unsigned MAX_SUBSCRIBERS = 16;
	unsigned CONTAINER_TEST_COUNT = 1 * 1000;
	unsigned CONTAINER_TEST_ITEM_COUNT = 5 * 1000;

	unsigned CONCURRENCY_TEST_ITEM_COUNT = 50 * 1000;
#else
	unsigned VOLATILE_TEST_COUNT = 1 * 1000;
	unsigned PERSISTEN_TEST_COUNT = 5 * 1000;
	unsigned MAX_CLIENTS = 64;

	//
	// There is a TCP limit on how many connections we can make at same time,
	// so we can't add much than that
	//
	unsigned CONNECTION_STRESS_TEST_COUNT = 5 * 1000;
	unsigned MAX_SUBSCRIBERS = 64;
	unsigned CONTAINER_TEST_COUNT = 1 * 1000;
	unsigned CONTAINER_TEST_ITEM_COUNT = 5 * 1000;

	unsigned CONCURRENCY_TEST_ITEM_COUNT = 50 * 1000;
#endif

	auto hostname = "localhost";

	try
	{
		//
		// LOTS OF CONTAINERS
		//
		TEST_data_stress_test(hostname,
			MAX_CLIENTS,
			MAX_SUBSCRIBERS,
			CONTAINER_TEST_ITEM_COUNT);

		//
		// DEADLOCK ON DISCONNECT
		//
		TEST_deadlock_on_disconnect(hostname);

		//
		// LOTS OF CONNECTIONS
		//
		TEST_connection_stress(hostname, CONNECTION_STRESS_TEST_COUNT);

		//
		// CONCURRENCY
		//
		TEST_container_concurrency(
			hostname,
			MAX_CLIENTS,
			CONCURRENCY_TEST_ITEM_COUNT,
			"volatile_map",
			map_perf_test_c);

		TEST_container_concurrency(
			hostname,
			MAX_CLIENTS,
			CONCURRENCY_TEST_ITEM_COUNT,
			"volatile_list",
			vector_perf_test_c);



		return 0;


	}
	catch (std::exception& ex)
	{
		cout << "EXCEPTION: " << ex.what() << endl;
		return -1;
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