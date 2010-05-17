
#include "pch.h"
#include "TioTcpServer.h"
#include "Container.h"
#include "MemoryStorage.h"
//#include "BdbStorage.h"
#include "LogDbStorage.h"


using namespace tio;
using boost::shared_ptr;

void LoadStorageTypes(ContainerManager* containerManager, const string& dataPath)
{
	shared_ptr<ITioStorageManager> mem = 
		shared_ptr<ITioStorageManager>(new tio::MemoryStorage::MemoryStorageManager());
	
	shared_ptr<ITioStorageManager> ldb = 
		shared_ptr<ITioStorageManager>(new tio::LogDbStorage::LogDbStorageManager(dataPath));

	containerManager->RegisterFundamentalStorageManagers(mem, mem);

//	containerManager->RegisterStorageManager("bdb_map", bdb);
//	containerManager->RegisterStorageManager("bdb_vector", bdb);

	containerManager->RegisterStorageManager("persistent/list", ldb);
	containerManager->RegisterStorageManager("persistent/map", ldb);
}

void TestEventSink(const string& eventName, const TioData* key, const TioData* value, const TioData* metadata)
{
	std::cout << eventName << "(" << key << ", " << value << ", " << metadata << ")" << endl;
}

/*
int StressTest()
{
	static const unsigned int TEST_DATA_STORE_COUNT =  100;
	static const unsigned int TEST_RECORD_COUNT =  500;
	static const unsigned int TEST_SINK_COUNT =  10;
	using boost::lexical_cast;
	using boost::shared_ptr;
	using namespace tio;

	TestEventSink sinks[TEST_SINK_COUNT];

	tio::ContainerManager containerManager;
	LoadStorageTypes(&containerManager, "c:\\temp\\tiodata");

	Timer t;

	{
		for(int test = 0 ; test < TEST_DATA_STORE_COUNT; ++test)
		{
			shared_ptr<ITioContainer> ds;
			string name = string("abc_") + lexical_cast<string>(test);
			ds = containerManager.CreateContainer("vector", name);

			for(int b = 0 ; b < TEST_SINK_COUNT ; ++b)
			{
				sinks[b].SetName(name);
				ds->Subscribe(&sinks[b], false);
			}

			for(int a = 0 ; a < TEST_RECORD_COUNT ; ++a)
			{
				ds->SetProperty(string("key") + lexical_cast<string>(a),
					string("value") + lexical_cast<string>(a));

				TioData key, value, metadata;
				
				key.Set(0); 
				value.Set(a*2) ; 
				metadata.Set(lexical_cast<string>(a).c_str(), true);

				ds->PushBack(NULL, &value, &metadata);
				ds->PushFront(NULL, &value, &metadata);
				ds->Insert(&key, &value, &metadata);
			}
		}
	}

	std::cout << t.Elapsed() << " ms (" 
		<< TEST_DATA_STORE_COUNT << " data containers, " << TEST_RECORD_COUNT << " records)" << std::endl;

	return 0;
}
*/

void RunServer(const string& dataPath, unsigned short port, 
			   const vector< pair<string, string> >& users,
			   const vector< pair<string, string> >& aliases)
{
	namespace asio = boost::asio;
	using namespace boost::asio::ip;

#ifndef _WIN32
	//ProfilerStart("/tmp/tio.prof");
#endif

	asio::io_service io_service;
	tio::ContainerManager containerManager;
	tcp::endpoint e(tcp::v4(), port);

	LoadStorageTypes(&containerManager, dataPath);

	
	pair<string, string> p;
	BOOST_FOREACH(p, aliases)
	{
		containerManager.AddAlias(p.first, p.second);
	}

	//
	// default aliases
	//
	
	if(users.size())
	{
		shared_ptr<ITioContainer> usersContainer = containerManager.CreateContainer("volatile/map", "__users__");

		pair<string, string> p;
		BOOST_FOREACH(p, users)
		{
			usersContainer->Set(p.first, p.second, "clean");
		}
	}

	tio::TioTcpServer tioServer(containerManager, io_service, e);
	
	tioServer.Start();

	cout << "running!" << endl;

	io_service.run();

#ifndef _WIN32
	//ProfilerStop();
#endif
}


int main(int argc, char* argv[])
{
	using namespace std;
	namespace po = boost::program_options;

	try
	{
		po::options_description desc("Options");

		desc.add_options()
			("alias", po::value< vector<string> >(), "set an alias for a container type, using syntax alias:container_type")
			("user", po::value< vector<string> >(), "add user, using syntax user:password")
			("port", po::value<unsigned short>(), "listening port")
			("data-path", po::value<string>(), "sets data path");

		po::variables_map vm;
		po::store(po::parse_command_line(argc, argv, desc), vm);
		po::notify(vm);

		if(vm.count("data-path") == 0 || vm.count("port") == 0)
		{
			cout << desc << endl;
			return 1;
		}

		vector< pair<string, string> > aliases;

		if(vm.count("alias") != 0)
		{
			BOOST_FOREACH(const string& alias, vm["alias"].as< vector<string> >())
			{
				string::size_type sep = alias.find(':', 0);

				if(sep == string::npos)
				{
					cout << "invalid alias: \"" << alias << "\"" << endl;
					return 1;
				}

				aliases.push_back(make_pair(alias.substr(0, sep), alias.substr(sep+1)));
			}
		}

		vector< pair<string, string> > users;

		if(vm.count("user") != 0)
		{
			BOOST_FOREACH(const string& user, vm["user"].as< vector<string> >())
			{
				string::size_type sep = user.find(':', 0);

				if(sep == string::npos)
				{
					cout << "invalid user syntax: \"" << user << "\"" << endl;
					return 1;
				}

				users.push_back(make_pair(user.substr(0, sep), user.substr(sep+1)));
			}
		}
		
		cout << "Starting tio server... ";
		
		RunServer(
			vm["data-path"].as<string>(),
			vm["port"].as<unsigned short>(),
			users,
			aliases);
	}
	catch(std::exception& ex)
	{
		cout << "error: " << ex.what() << endl;
	}

	return 0;
}

