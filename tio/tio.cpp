/*
Tio: The Information Overlord
Copyright 2010 Rodrigo Strauss (http://www.1bit.com.br)

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#include "pch.h"
#include "TioTcpServer.h"
#include "Container.h"
#include "MemoryStorage.h"
//#include "BdbStorage.h"
#include "LogDbStorage.h"
#include "TioPython.h"
#include "../tioclient/tioclient.hpp"


using namespace tio;

using boost::shared_ptr;
using boost::scoped_array;
using std::cout;
using std::endl;
using std::queue;

void LoadStorageTypes(ContainerManager* containerManager, const string& dataPath)
{
	shared_ptr<ITioStorageManager> mem = 
		shared_ptr<ITioStorageManager>(new tio::MemoryStorage::MemoryStorageManager());
	
	shared_ptr<ITioStorageManager> ldb = 
		shared_ptr<ITioStorageManager>(new tio::LogDbStorage::LogDbStorageManager(dataPath));

	containerManager->RegisterFundamentalStorageManagers(mem, mem);

//	containerManager->RegisterStorageManager("bdb_map", bdb);
//	containerManager->RegisterStorageManager("bdb_vector", bdb);

	containerManager->RegisterStorageManager("persistent_list", ldb);
	containerManager->RegisterStorageManager("persistent_map", ldb);
}

void SetupContainerManager(
	tio::ContainerManager* manager, 
	const string& dataPath,
	const vector< pair<string, string> >& aliases)
{
	LoadStorageTypes(manager, dataPath);

	pair<string, string> p;
	BOOST_FOREACH(p, aliases)
	{
		manager->AddAlias(p.first, p.second);
	}
}

void RunServer(tio::ContainerManager* manager,
			   unsigned short port, 
			   const vector< pair<string, string> >& users)
{
	namespace asio = boost::asio;
	using namespace boost::asio::ip;

#ifndef _WIN32
	//ProfilerStart("/tmp/tio.prof");
#endif

	asio::io_service io_service;
	tcp::endpoint e(tcp::v4(), port);

	//
	// default aliases
	//
	if(users.size())
	{
		shared_ptr<ITioContainer> usersContainer = manager->CreateContainer("volatile_map", "__users__");

		pair<string, string> p;
		BOOST_FOREACH(p, users)
		{
			usersContainer->Set(p.first, p.second, "clean");
		}
	}

	tio::TioTcpServer tioServer(*manager, io_service, e);

	cout << "running on port " << port << "." << endl;

	tioServer.Start();

	io_service.run();

#ifndef _WIN32
	//ProfilerStop();
#endif
}

class cpp2c
{
	TIO_DATA c_;
public:
	cpp2c(const TioData& v)
	{
		tiodata_set_as_none(&c_);

		switch(v.GetDataType())
		{
		case TioData::Sz:
			tiodata_set_string(&c_, v.AsSz());
			break;
		case TioData::Int:
			tiodata_set_int(&c_, v.AsInt());
			break;
		case TioData::Double:
			tiodata_set_double(&c_, v.AsDouble());
			break;
		}
	}

	operator TIO_DATA*()
	{
		return &c_;
	}

};

class c2cpp
{
	TioData cpp_;
	TIO_DATA* c_;
	bool out_;
public:
	c2cpp(const TIO_DATA* c)
	{
		c_ = const_cast<TIO_DATA*>(c);
		out_ = false;

		if(!c_)
			return;

		switch(c->data_type)
		{
		case TIO_DATA_TYPE_DOUBLE:
			cpp_.Set(c->double_);
			break;
		case TIO_DATA_TYPE_INT:
			cpp_.Set(c->int_);
			break;
		case TIO_DATA_TYPE_STRING:
			cpp_.Set(c->string_, false);
			break;
		}
	}

	~c2cpp()
	{
		if(out_ && c_)
		{
			switch(cpp_.GetDataType())
			{
			case TioData::Sz:
				tiodata_set_string(c_, cpp_.AsSz());
				break;
			case TioData::Int:
				tiodata_set_int(c_, cpp_.AsInt());
				break;
			case TioData::Double:
				tiodata_set_double(c_, cpp_.AsDouble());
				break;
			}
		}
	}

	operator TioData()
	{
		return cpp_;
	}

	TioData* inptr()
	{
		if(cpp_.IsNull())
			return NULL;

		return &cpp_;
	}

	TioData* outptr()
	{
		if(!c_)
			return NULL;

		out_ = true;
		return &cpp_;
	}
};

class LocalContainerManager : public IContainerManager
{
private:
	tio::ContainerManager& containerManager_;
	std::map<void*, unsigned int> subscriptionHandles_;

public:

	LocalContainerManager(tio::ContainerManager& containerManager) 
		:
	    containerManager_(containerManager)
	{
	}

	IContainerManager* container_manager()
	{
		return this;
	}

protected:
	virtual int create(const char* name, const char* type, void** handle)
	{
		try
		{
			shared_ptr<ITioContainer> container = containerManager_.CreateContainer(type, name);
			*handle = new shared_ptr<ITioContainer>(container);
		}
		catch(std::exception&)
		{
			return -1;
		}

		return 0;
	}

	virtual int open(const char* name, const char* type, void** handle)
	{
		try
		{
			shared_ptr<ITioContainer> container = containerManager_.OpenContainer(type ? std::string(type) : string(), name);
			*handle = new shared_ptr<ITioContainer>(container);
		}
		catch(std::exception&)
		{
			return -1;
		}

		return 0;
	}

	virtual int close(void* handle)
	{
		delete ((shared_ptr<ITioContainer>*)handle);
		return 0;
	}

	virtual int container_propset(void* handle, const struct TIO_DATA* key, const struct TIO_DATA* value)
	{
		ITioContainer* container = ((shared_ptr<ITioContainer>*)handle)->get();
		
		if(key->data_type != TIO_DATA_TYPE_STRING || value->data_type != TIO_DATA_TYPE_STRING)
			return -1;

		container->SetProperty(key->string_, value->string_);

		return 0;
	}

	virtual int container_push_back(void* handle, const struct TIO_DATA* key, const struct TIO_DATA* value, const struct TIO_DATA* metadata)
	{
		ITioContainer* container = ((shared_ptr<ITioContainer>*)handle)->get();
		
		try
		{
			container->PushBack(c2cpp(key), c2cpp(value), c2cpp(metadata));
		}
		catch(std::exception&)
		{
			return -1;
		}

		return 0;
	}

	virtual int container_push_front(void* handle, const struct TIO_DATA* key, const struct TIO_DATA* value, const struct TIO_DATA* metadata)
	{
		ITioContainer* container = ((shared_ptr<ITioContainer>*)handle)->get();

		try
		{
			container->PushFront(c2cpp(key), c2cpp(value), c2cpp(metadata));
		}
		catch(std::exception&)
		{
			return -1;
		}

		return 0;
	}

	virtual int container_pop_back(void* handle, struct TIO_DATA* key, struct TIO_DATA* value, struct TIO_DATA* metadata)
	{
		ITioContainer* container = ((shared_ptr<ITioContainer>*)handle)->get();

		try
		{
			container->PopBack(c2cpp(key).outptr(), c2cpp(value).outptr(), c2cpp(metadata).outptr());
		}
		catch(std::exception&)
		{
			return -1;
		}

		return 0;
	}

	virtual int container_pop_front(void* handle, struct TIO_DATA* key, struct TIO_DATA* value, struct TIO_DATA* metadata)
	{
		ITioContainer* container = ((shared_ptr<ITioContainer>*)handle)->get();

		try
		{
			container->PopFront(c2cpp(key).outptr(), c2cpp(value).outptr(), c2cpp(metadata).outptr());
		}
		catch(std::exception&)
		{
			return -1;
		}

		return 0;
	}

	virtual int container_set(void* handle, const struct TIO_DATA* key, const struct TIO_DATA* value, const struct TIO_DATA* metadata)
	{
		ITioContainer* container = ((shared_ptr<ITioContainer>*)handle)->get();

		try
		{
			container->Set(c2cpp(key), c2cpp(value), c2cpp(metadata));
		}
		catch(std::exception&)
		{
			return -1;
		}

		return 0;
	}

	virtual int container_insert(void* handle, const struct TIO_DATA* key, const struct TIO_DATA* value, const struct TIO_DATA* metadata)
	{
		ITioContainer* container = ((shared_ptr<ITioContainer>*)handle)->get();

		try
		{
			container->Insert(c2cpp(key), c2cpp(value), c2cpp(metadata));
		}
		catch(std::exception&)
		{
			return -1;
		}

		return 0;
	}

	virtual int container_clear(void* handle)
	{
		ITioContainer* container = ((shared_ptr<ITioContainer>*)handle)->get();

		try
		{
			container->Clear();
		}
		catch(std::exception&)
		{
			return -1;
		}

		return 0;
	}

	virtual int container_delete(void* handle, const struct TIO_DATA* key)
	{
		ITioContainer* container = ((shared_ptr<ITioContainer>*)handle)->get();

		try
		{
			container->Delete(c2cpp(key));
		}
		catch(std::exception&)
		{
			return -1;
		}

		return 0;
	}

	virtual int container_get(void* handle, const struct TIO_DATA* search_key, struct TIO_DATA* key, struct TIO_DATA* value, struct TIO_DATA* metadata)
	{
		ITioContainer* container = ((shared_ptr<ITioContainer>*)handle)->get();

		try
		{
			container->GetRecord(c2cpp(search_key), c2cpp(key).outptr(), c2cpp(value).outptr(), c2cpp(metadata).outptr());
		}
		catch(std::exception&)
		{
			return -1;
		}

		return 0;
	}

	virtual int container_propget(void* handle, const struct TIO_DATA* search_key, struct TIO_DATA* value)
	{
		ITioContainer* container = ((shared_ptr<ITioContainer>*)handle)->get();
		std::string ret;

		try
		{
			ret = container->GetProperty(static_cast<TioData>(c2cpp(search_key)).AsSz());
		}
		catch(std::exception&)
		{
			return -1;
		}

		tiodata_set_string(value, ret.c_str());

		return 0;
	}

	virtual int container_get_count(void* handle, int* count)
	{
		ITioContainer* container = ((shared_ptr<ITioContainer>*)handle)->get();

		try
		{
			*count = container->GetRecordCount();
		}
		catch(std::exception&)
		{
			return -1;
		}

		return 0;
	}

	virtual int container_query(void* handle, int start, int end, query_callback_t query_callback, void* cookie)
	{
		ITioContainer* container = ((shared_ptr<ITioContainer>*)handle)->get();

		try
		{
			shared_ptr<ITioResultSet> resultset = container->Query(start, end, TIONULL);

			TioData key, value, metadata;

			while(resultset->GetRecord(&key, &value, &metadata))
			{
				query_callback(cookie, 0, cpp2c(key), cpp2c(value), cpp2c(metadata));
				resultset->MoveNext();
			}
		}
		catch(std::exception&)
		{
			return -1;
		}

		return 0;
	}

	static void SubscribeBridge(void* cookie, event_callback_t event_callback, const string& eventName, const TioData& key, const TioData& value, const TioData& metadata)
	{
		event_callback(cookie, 10, 0, cpp2c(key), cpp2c(value), cpp2c(metadata));
	}

	virtual int container_subscribe(void* handle, struct TIO_DATA* start, event_callback_t event_callback, void* cookie)
	{
		ITioContainer* container = ((shared_ptr<ITioContainer>*)handle)->get();

		string startString;

		if(start && start->data_type != TIO_DATA_TYPE_STRING)
			return -1;

		if(start)
			startString = start->string_;

		unsigned int cppHandle;

		try
		{
			cppHandle = container->Subscribe(boost::bind(&LocalContainerManager::SubscribeBridge, cookie, event_callback, _1, _2, _3, _4), startString);
			subscriptionHandles_[handle] = cppHandle;
		}
		catch(std::exception&)
		{
			return -1;
		}

		return 0;
	}

	virtual int container_unsubscribe(void* handle)
	{
		ITioContainer* container = ((shared_ptr<ITioContainer>*)handle)->get();

		try
		{
			container->Unsubscribe(subscriptionHandles_[handle]);
		}
		catch(std::exception&)
		{
			return -1;
		}

		return 0;
	}
};

class PluginThread
{
	tio_plugin_start_t func_;
	tio::IContainerManager* containerManager_;
	queue<boost::function<void()> > eventQueue_;
	boost::condition_variable hasWork_;

public:
	PluginThread(tio_plugin_start_t func, tio::IContainerManager* containerManager) : func_(func), containerManager_(containerManager)
	{

	}

	void AnyThreadCallback(EventSink sink, const string& eventName, const TioData& key, const TioData& value, const TioData& metadata)
	{

	}

	void start()
	{
		
		
	}
};

#ifdef _WIN32	
void LoadPlugin(const string path, tio::IContainerManager* containerManager, const map<string, string>& pluginParameters)
{
	tio_plugin_start_t pluginStartFunction = NULL;

	HMODULE hdll = LoadLibrary(path.c_str());
	
	if(!hdll)
	{
		stringstream str;
		str << "error loading plugin \"" << path << "\": Win32 error " << GetLastError();
		throw std::runtime_error(str.str());
	}

	pluginStartFunction = (tio_plugin_start_t)GetProcAddress(hdll, "tio_plugin_start");

	if(!pluginStartFunction)
	{
		throw std::runtime_error("plugin doesn't export the \"tio_plugin_start\"");
	}

	scoped_array<KEY_AND_VALUE> kv(new KEY_AND_VALUE[pluginParameters.size() + 1]);

	int a = 0;
	for(map<string, string>::const_iterator i = pluginParameters.begin() ; i != pluginParameters.end() ; ++i, a++)
	{
		kv[a].key = i->first.c_str();
		kv[a].value = i->second.c_str();
	}

	// last one must have key = NULL
	kv[pluginParameters.size()].key = NULL;

	pluginStartFunction(containerManager, kv.get());
}
#endif //_WIN32

void LoadPlugins(const std::vector<std::string>& plugins, const map<string, string>& pluginParameters, tio::IContainerManager* containerManager)
{
	BOOST_FOREACH(const string& pluginPath, plugins)
	{
		LoadPlugin(pluginPath, containerManager, pluginParameters);
	}
}

int main(int argc, char* argv[])
{
	namespace po = boost::program_options;

	cout << "Tio, The Information Overlord. Copyright Rodrigo Strauss (www.1bit.com.br)" << endl;

	try
	{
		po::options_description desc("Options");

		desc.add_options()
			("alias", po::value< vector<string> >(), "set an alias for a container type, using syntax alias=container_type")
			("user", po::value< vector<string> >(), "add user, using syntax user:password")
			("python-plugin", po::value< vector<string> >(), "load and run a python plugin")
			("plugin", po::value< vector<string> >(), "load and run a plugin")
			("plugin-parameter", po::value< vector<string> >(), "parameters to be passed to plugins. name=value")
			("port", po::value<unsigned short>(), "listening port")
			("threads", po::value<unsigned short>(), "number of running threads")
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
				string::size_type sep = alias.find('=', 0);

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

		{
			cout << "Starting Tio Infrastructure... " << endl;
			tio::ContainerManager containerManager;
			LocalContainerManager localContainerManager(containerManager);
			
			SetupContainerManager(&containerManager, vm["data-path"].as<string>(), aliases);

			//
			// Parse plugin parameters
			//
			map<string, string> pluginParameters;

			BOOST_FOREACH(const string& parameter, vm["plugin-parameter"].as< vector<string> >())
			{
				string::size_type sep = parameter.find('=', 0);

				if(sep == string::npos)
				{
					cout << "invalid plugin parameter syntax: \"" << parameter << "\"" << endl;
					return 1;
				}

				pluginParameters[parameter.substr(0, sep)] = parameter.substr(sep+1);
			}

			if(vm.count("plugin"))
			{
				cout << "Loading plugins... " << endl;
				LoadPlugins(vm["plugin"].as< vector<string> >(), pluginParameters, &localContainerManager);
			}

			if(vm.count("python-plugin"))
			{
				cout << "Starting Python support... " << endl;
				InitializePythonSupport(argv[0], &containerManager);

				cout << "Loading Python plugins... " << endl;
				LoadPythonPlugins(vm["python-plugin"].as< vector<string> >(), pluginParameters);
			}
		
			RunServer(
				&containerManager,
				vm["port"].as<unsigned short>(),
				users);
		}
	}
	catch(std::exception& ex)
	{
		cout << "error: " << ex.what() << endl;
	}

	return 0;
}

