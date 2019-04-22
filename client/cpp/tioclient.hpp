#pragma once
#include "../c/tioclient.h"
#include <string>
#include <sstream>
#include <functional>


namespace tio
{
	using std::string;
	using std::stringstream;
	using std::runtime_error;

	class tio_exception : public std::runtime_error
	{
		int errorCode_;
	public:

		explicit tio_exception(int errorCode, const string& message)
			: std::runtime_error(message)
			, errorCode_(errorCode)
		{}

		int code()
		{
			return errorCode_;
		}
	};

	inline void ToTioData(int v, TIO_DATA* tiodata)
	{
		tiodata_set_as_none(tiodata);
		tiodata_set_int(tiodata, v);
	}

	//
	// TODO: tio doesn't support unsigned int. But STL containers indexes
	// are size_t's
	// 
	inline void ToTioData(unsigned int v, TIO_DATA* tiodata)
	{
		tiodata_set_as_none(tiodata);
		tiodata_set_int(tiodata, (int)v);
	}

	inline void ToTioData(const string& v, TIO_DATA* tiodata)
	{
		tiodata_set_as_none(tiodata);
		tiodata_set_string_and_size(tiodata, v.c_str(), v.size());
	}

	inline void FromTioData(const TIO_DATA* tiodata, int* value)
	{
		if(tiodata->data_type != TIO_DATA_TYPE_INT)
			throw runtime_error("wrong data type");

		*value = tiodata->int_;
	}

	//
	// TODO: cast maybe not correct...
	//
	inline void FromTioData(const TIO_DATA* tiodata, unsigned int* value)
	{
		if(tiodata->data_type != TIO_DATA_TYPE_INT)
			throw runtime_error("wrong data type");

		*value = static_cast<unsigned int>(tiodata->int_);
	}

	inline void FromTioData(const TIO_DATA* tiodata, string* value)
	{
		if(tiodata->data_type != TIO_DATA_TYPE_STRING)
			throw runtime_error("wrong data type");

		value->assign(tiodata->string_, tiodata->string_size_);
	}

	inline void ThrowOnTioClientError(int result)
	{
		//
		// TODO: create a typed exception an fill it accordingly
		// 
		if(result < 0)
		{
			stringstream str;
			str << "client error " << result << ": \"" << tio_get_last_error_description() << "\"" ;
			throw tio_exception(result, str.str());
		}
	}

	template<typename TValue>
	class TioDataConverter
	{
		TIO_DATA tiodata_;
	public:

		TioDataConverter(const TioDataConverter& rhv)
		{
			tiodata_init(&tiodata_);
			
			*this = rhv;
		}

		TioDataConverter& operator=(const TioDataConverter& rhv)
		{
			tiodata_set_as_none(&tiodata_);
			tiodata_copy(&rhv.tiodata_, &tiodata_);
			return *this;
		}

		TioDataConverter(const TioDataConverter&& rhv)
		{
			tiodata_ = rhv.tiodata_;
			rhv.tiodata_.data_type = TIO_DATA_TYPE_NONE;
		}

		TioDataConverter()
		{
			tiodata_init(&tiodata_);
		}
		
		~TioDataConverter()
		{
			tiodata_set_as_none(&tiodata_);
		}

		explicit TioDataConverter(const TValue& v)
		{
			tiodata_init(&tiodata_);
			ToTioData(v, &tiodata_);
		}

		explicit TioDataConverter(const TValue* v)
		{
			tiodata_init(&tiodata_);
			
			//
			// Type will be TIO_DATA_TYPE_NONE if the pointer is null
			//
			if(v)
				ToTioData(*v, &tiodata_);
		}

		const TIO_DATA* inptr() const
		{
			return &tiodata_;
		}

		TIO_DATA* outptr()
		{
			tiodata_set_as_none(&tiodata_);
			return &tiodata_;
		}

		TValue value() const
		{
			TValue v;
			
			//
			// If you got an "could not convert "error on this line, you need to provide
			// a specialization of the FromTioData to you data type. Check other examples
			// on this file
			//
			FromTioData(&tiodata_, &v);
			return v;
		}
	};

	struct IContainerManager
	{
		virtual int create(const char* name, const char* type, void** handle)=0;
		virtual int open(const char* name, const char* type, void** handle)=0;
		virtual int close(void* handle)=0;

		virtual int group_add(const char* group_name, const char* container_name)=0;

		virtual int container_propset(void* handle, const struct TIO_DATA* key, const struct TIO_DATA* value)=0;
		virtual int container_push_back(void* handle, const struct TIO_DATA* key, const struct TIO_DATA* value, const struct TIO_DATA* metadata)=0;
		virtual int container_push_front(void* handle, const struct TIO_DATA* key, const struct TIO_DATA* value, const struct TIO_DATA* metadata)=0;
		virtual int container_pop_back(void* handle, struct TIO_DATA* key, struct TIO_DATA* value, struct TIO_DATA* metadata)=0;
		virtual int container_pop_front(void* handle, struct TIO_DATA* key, struct TIO_DATA* value, struct TIO_DATA* metadata)=0;
		virtual int container_set(void* handle, const struct TIO_DATA* key, const struct TIO_DATA* value, const struct TIO_DATA* metadata)=0;
		virtual int container_insert(void* handle, const struct TIO_DATA* key, const struct TIO_DATA* value, const struct TIO_DATA* metadata)=0;
		virtual int container_clear(void* handle)=0;
		virtual int container_delete(void* handle, const struct TIO_DATA* key)=0;
		virtual int container_get(void* handle, const struct TIO_DATA* search_key, struct TIO_DATA* key, struct TIO_DATA* value, struct TIO_DATA* metadata)=0;
		virtual int container_propget(void* handle, const struct TIO_DATA* search_key, struct TIO_DATA* value)=0;
		virtual int container_get_count(void* handle, int* count)=0;
		virtual int container_query(void* handle, int start, int end, query_callback_t query_callback, void* cookie)=0;
		virtual int container_subscribe(void* handle, struct TIO_DATA* start, event_callback_t event_callback, void* cookie)=0;
		virtual int container_unsubscribe(void* handle)=0;
		virtual int container_wait_and_pop_next(void* handle, event_callback_t event_callback, void* cookie)=0;
		virtual bool connected()=0;

		virtual IContainerManager* container_manager()=0;
	};



	class Connection : private IContainerManager
	{
		TIO_CONNECTION* connection_;
		string host_;
		short port_;

	
	protected:

		virtual int create(const char* name, const char* type, void** handle)
		{
			return tio_create(connection_, name, type, (TIO_CONTAINER**)handle);
		}

		virtual int open(const char* name, const char* type, void** handle)
		{
			return tio_open(connection_, name, type, (TIO_CONTAINER**)handle);
		}

		virtual int group_add(const char* group_name, const char* container_name)
		{
			return tio_group_add(connection_, group_name, container_name);
		}

		virtual int close(void* handle)
		{
			return tio_close((TIO_CONTAINER*)handle);
		}

		virtual int container_propset(void* handle, const struct TIO_DATA* key, const struct TIO_DATA* value)
		{
			return tio_container_propset((TIO_CONTAINER*)handle, key, value);
		}

		virtual int container_push_back(void* handle, const struct TIO_DATA* key, const struct TIO_DATA* value, const struct TIO_DATA* metadata)
		{
			return tio_container_push_back((TIO_CONTAINER*)handle, key, value, metadata);
		}

		virtual int container_push_front(void* handle, const struct TIO_DATA* key, const struct TIO_DATA* value, const struct TIO_DATA* metadata)
		{
			return tio_container_push_front((TIO_CONTAINER*)handle, key, value, metadata);
		}

		virtual int container_pop_back(void* handle, struct TIO_DATA* key, struct TIO_DATA* value, struct TIO_DATA* metadata)
		{
			return tio_container_pop_back((TIO_CONTAINER*)handle, key, value, metadata);
		}

		virtual int container_pop_front(void* handle, struct TIO_DATA* key, struct TIO_DATA* value, struct TIO_DATA* metadata)
		{
			return tio_container_pop_front((TIO_CONTAINER*)handle, key, value, metadata);
		}

		virtual int container_set(void* handle, const struct TIO_DATA* key, const struct TIO_DATA* value, const struct TIO_DATA* metadata)
		{
			return tio_container_set((TIO_CONTAINER*)handle, key, value, metadata);
		}

		virtual int container_insert(void* handle, const struct TIO_DATA* key, const struct TIO_DATA* value, const struct TIO_DATA* metadata)
		{
			return tio_container_insert((TIO_CONTAINER*)handle, key, value, metadata);
		}

		virtual int container_clear(void* handle)
		{
			return tio_container_clear((TIO_CONTAINER*)handle);
		}

		virtual int container_delete(void* handle, const struct TIO_DATA* key)
		{
			return tio_container_delete((TIO_CONTAINER*)handle, key);
		}

		virtual int container_get(void* handle, const struct TIO_DATA* search_key, struct TIO_DATA* key, struct TIO_DATA* value, struct TIO_DATA* metadata)
		{
			return tio_container_get((TIO_CONTAINER*)handle, search_key, key, value, metadata);
		}

		virtual int container_propget(void* handle, const struct TIO_DATA* search_key, struct TIO_DATA* value)
		{
			return tio_container_propget((TIO_CONTAINER*)handle, search_key, value);
		}

		virtual int container_get_count(void* handle, int* count)
		{
			return tio_container_get_count((TIO_CONTAINER*)handle, count);
		}

		virtual int container_query(void* handle, int start, int end, query_callback_t query_callback, void* cookie)
		{
			return tio_container_query((TIO_CONTAINER*)handle, start, end, nullptr, query_callback, cookie);
		}

		virtual int container_subscribe(void* handle, struct TIO_DATA* start, event_callback_t event_callback, void* cookie)
		{
			return tio_container_subscribe((TIO_CONTAINER*)handle, start, event_callback, cookie);
		}

		virtual int container_unsubscribe(void* handle)
		{
			return tio_container_unsubscribe((TIO_CONTAINER*)handle);
		}

		virtual int container_wait_and_pop_next(void* handle, event_callback_t event_callback, void* cookie)
		{
			return tio_container_wait_and_pop_next((TIO_CONTAINER*)handle, event_callback, cookie);
		}

		void BeginNetworkBatch()
		{
			tio_begin_network_batch(connection_);
		}

		void FinishNetworkBatch()
		{
			tio_finish_network_batch(connection_);
		}

	public:

		class TioScopedNetworkBatch : boost::noncopyable
		{
			Connection& connection_;
		public:
			TioScopedNetworkBatch(Connection& connection)
				: connection_(connection)
			{
				connection_.BeginNetworkBatch();
			}

			~TioScopedNetworkBatch()
			{
				connection_.FinishNetworkBatch();
			}
		};


		Connection() : connection_(nullptr), port_(0)
		{
			tio_initialize();
		}

		Connection(const string& host, short port = 2605) : connection_(nullptr), port_(0)
		{
			tio_initialize();
			Connect(host, port);
		}

		~Connection()
		{
			Disconnect();
		}

		virtual IContainerManager* container_manager()
		{
			return this;
		}

		string host() { return host_;}
		short port() { return port_;}

		void Connect(const string& host, short port = 2605)
		{
			int result;

			Disconnect();

			result = tio_connect(host.c_str(), port, &connection_);

			ThrowOnTioClientError(result);

			host_ = host;
			port_ = port;
		}

		void Disconnect()
		{
			tio_disconnect(connection_);
			connection_ = nullptr;
		}

		int WaitForNextEventAndDispatch(unsigned int timeOutInSeconds)
		{
			int ret;

			tio_dispatch_pending_events(connection_, 0xFFFFFFFF);

			ret = tio_receive_next_pending_event(connection_, &timeOutInSeconds);

			if(ret == TIO_ERROR_TIMEOUT)
			{
				return 0;
			}
			else
			{
				ThrowOnTioClientError(ret);
			}

			tio_dispatch_pending_events(connection_, 0xFFFFFFFF);

			return ret;
		}

		bool connected()
		{
			return !!cnptr();
		}


		TIO_CONNECTION* cnptr()
		{
			return connection_;
		}

		void AddToGroup(const string& groupName, const string& containerName)
		{
			int ret = tio_group_add(connection_, groupName.c_str(), containerName.c_str());

			ThrowOnTioClientError(ret);
		}
	};

	template<typename TContainer, typename TKey, typename TValue>
	class ServerValue
	{
		TKey key_;
		TContainer& container_;

		typedef ServerValue<TContainer, TKey, TValue> this_type;
	public:
		ServerValue(TContainer& container, const TKey& key) :
			container_(container),
			key_(key)
		{}

		TValue value()
		{
			return container_.at(key_);
		}

		operator TValue()
		{
			return container_.at(key_);
		}

		this_type& operator=(const TValue& v)
		{
			container_.set(key_, v);
			return *this;
		}

		bool operator==(const TValue& v)
		{
			return value() == v;
		}

		bool operator==(const this_type& v)
		{
			return value() == v.value();
		}
	};


	namespace containers
	{
		template<typename TKey, typename TValue, typename TMetadata, typename SelfT>
		class TioContainerImpl 
		{
		private: 
			// non copyable
			TioContainerImpl(const TioContainerImpl&){}
			TioContainerImpl& operator=(const TioContainerImpl&){return *this;}

		public:
			typedef TioContainerImpl<TKey, TValue, TMetadata, SelfT> this_type;
			typedef TKey key_type;
			typedef TValue value_type;
			typedef TMetadata metadata_type;
			typedef ServerValue<SelfT, TKey, TValue> server_value_type;
			typedef std::function<void (const string& /*containerName*/, const string& /*eventName*/, const TKey&, const TValue&)> EventCallbackT;

		protected:
			void* container_;
			std::string name_;
			IContainerManager* containerManager_;
			EventCallbackT eventCallback_;
			EventCallbackT waitAndPopNextCallback_;

			IContainerManager* container_manager()
			{
				if(!containerManager_)
					throw std::runtime_error("container closed");

				return containerManager_;
			}

		public:
			TioContainerImpl() :
				container_(nullptr),
				containerManager_(nullptr),
				eventCallback_(nullptr),
				waitAndPopNextCallback_(nullptr)
			{
			}

			~TioContainerImpl()
			{
				if(connected())
					unsubscribe();
			}

			bool connected()
			{
				return !!containerManager_ && containerManager_->connected() && container_;
			}

			TIO_CONTAINER* handle()
			{
				return (TIO_CONTAINER*)container_;
			}

			template<typename TConnection>
			void open(TConnection* cn, const string& name)
			{
				int result;

				containerManager_ = cn->container_manager();

				result = container_manager()->open(name.c_str(), nullptr, &container_);

				ThrowOnTioClientError(result);

				name_ = name;
			}


			template<typename TConnection>
			void create(TConnection* cn, const string& name, const string& type = "")
			{
				int result;

				close();

				containerManager_ = cn->container_manager();

				result = container_manager()->create(name.c_str(), type.c_str(), &container_);

				ThrowOnTioClientError(result);

				name_ = name;
			}

			void close()
			{
				if(!connected())
					return;

				container_manager()->close(container_);

				name_.clear();
			}


			static void EventCallback(int result, void* handle, void* cookie, unsigned int event_code, 
				const char* group_name, const char* container_name, 
				const struct TIO_DATA* key, const struct TIO_DATA* value, const struct TIO_DATA*)
			{
				this_type* me = (this_type*)cookie;
				TKey typedKey;
				TValue typedValue;

				if(key->data_type != TIO_DATA_TYPE_NONE)
					FromTioData(key, &typedKey);

				if(value->data_type != TIO_DATA_TYPE_NONE)
					FromTioData(value, &typedValue);
				
				me->eventCallback_(container_name, "event", typedKey, typedValue);
			}

			// typedef void (*event_callback_t)(void* /*cookie*/, const char* /*group_name*/, const char* /*container_name*/, unsigned int /*handle*/, unsigned int /*event_code*/, const struct TIO_DATA*, const struct TIO_DATA*, const struct TIO_DATA*);
			static void WaitAndPopNextCallback(void* cookie, const char* /*group_name*/, const char* container_name, unsigned int /*handle*/, unsigned int /*event_code*/, 
				const struct TIO_DATA* key, const struct TIO_DATA* value, const struct TIO_DATA*)
			{
				this_type* me = (this_type*)cookie;
				TKey typedKey;
				TValue typedValue;

				if(key->data_type != TIO_DATA_TYPE_NONE)
					FromTioData(key, &typedKey);

				if(value->data_type != TIO_DATA_TYPE_NONE)
					FromTioData(value, &typedValue);

				//
				// When calling the callback, our state must be cleared. Reentrant stuff...
				//
				EventCallbackT cb = me->waitAndPopNextCallback_;
				me->waitAndPopNextCallback_ = nullptr;
				
				cb("wnp_next", container_name, typedKey, typedValue);
			}

			bool wait_and_pop_next(EventCallbackT callback)
			{
				int result;

				if(!callback)
					throw std::runtime_error("wait_and_pop_next callback can't be null");

				if(waitAndPopNextCallback_)
					return false;

				waitAndPopNextCallback_ = callback;

				result = container_manager()->container_wait_and_pop_next(
					container_,
					&this_type::WaitAndPopNextCallback,
					this);

				return true;
			}

			void AddToGroup(const string& groupName)
			{
				container_manager()->group_add(groupName.c_str(), tio_container_name(container_));
			}

			void subscribe(EventCallbackT callback)
			{
				int result;

				eventCallback_ = callback;

				result = container_manager()->container_subscribe(
					container_,
					nullptr,
					&this_type::EventCallback,
					this);
			}

			void unsubscribe()
			{
				int result;

				result = container_manager()->container_unsubscribe(
					container_);

				// we'll ignore any error, because it can be called from the destructor
			}

			void propset(const string& key, const string& value)
			{
				int result;

				result = container_manager()->container_propset(
					container_, 
					TioDataConverter<string>(key).inptr(),
					TioDataConverter<string>(value).inptr());

				ThrowOnTioClientError(result);
			}

			string propget(const string& key)
			{
				int result;
				TioDataConverter<string> value;

				result = container_manager()->container_propget(
					container_, 
					TioDataConverter<string>(key).inptr(),
					value.outptr());

				ThrowOnTioClientError(result);

				return value.value();
			}

			const string& name()
			{
				return name_;
			}

			void insert(const key_type& key, const value_type& value)
			{
				int result;

				result = container_manager()->container_insert(
					container_, 
					TioDataConverter<key_type>(key).inptr(),
					TioDataConverter<value_type>(value).inptr(),
					nullptr);

				ThrowOnTioClientError(result);
			}

			void set(const key_type& key, const value_type& value, const std::string* metadata = nullptr)
			{
				int result;

				result = container_manager()->container_set(
					container_, 
					TioDataConverter<key_type>(key).inptr(),
					TioDataConverter<value_type>(value).inptr(),
					metadata ? TioDataConverter<std::string>(*metadata).inptr() : nullptr);

				ThrowOnTioClientError(result);
			}

			value_type at(const key_type& index)
			{
				int result;
				TioDataConverter<value_type> value;

				result = container_manager()->container_get(
					container_, 
					TioDataConverter<key_type>(index).inptr(),
					nullptr,
					value.outptr(),
					nullptr);

				ThrowOnTioClientError(result);

				return value.value();
			}

			value_type get(const key_type& index, const value_type& defaultValue)
			{
				int result;
				TioDataConverter<value_type> value;

				result = container_manager()->container_get(
					container_, 
					TioDataConverter<key_type>(index).inptr(),
					nullptr,
					value.outptr(),
					nullptr);

				if(result != 0)
					return defaultValue;

				return value.value();
			}

			void erase(const key_type& index)
			{
				int result;

				result = container_manager()->container_delete(
					container_, 
					TioDataConverter<key_type>(index).inptr());

				ThrowOnTioClientError(result);
			}

			server_value_type operator[](const key_type& key)
			{
				return server_value_type(*static_cast<SelfT*>(this), key);
			}

			void clear()
			{
				int result;

				result = container_manager()->container_clear(container_);

				ThrowOnTioClientError(result);
			}

			size_t size()
			{
				int result, count;

				result = container_manager()->container_get_count(
					container_, 
					&count);

				ThrowOnTioClientError(result);

				return static_cast<size_t>(count);
			}

			void add_to_group(const string& groupName)
			{
				container_manager()->group_add(groupName.c_str(), name_.c_str());
			}
		};
	
		template<typename TValue, typename TMetadata=std::string>
		class list : public TioContainerImpl<int, TValue, TMetadata, list<TValue, TMetadata> >
		{
		public:
			typedef list<TValue, TMetadata> this_type;
			typedef typename TioContainerImpl<int, TValue, TMetadata, list<TValue, TMetadata> >::value_type value_type;
		
		public:
			void push_back(const value_type& value)
			{
				int result;

				result = this->container_manager()->container_push_back(
					this->container_, 
					nullptr,
					TioDataConverter<TValue>(value).inptr(),
					nullptr);

				ThrowOnTioClientError(result);
			}

			void push_front(const value_type& value)
			{
				int result;

				result = this->container_manager()->container_push_front(
					this->container_, 
					nullptr,
					TioDataConverter<TValue>(value).inptr(),
					nullptr);

				ThrowOnTioClientError(result);
			}

			value_type pop_back()
			{
				int result;
				TioDataConverter<value_type> value;

				result = this->container_manager()->container_pop_back(
					this->container_, 
					nullptr,
					value.outptr(),
					nullptr);

				ThrowOnTioClientError(result);

				return value.value();
			}

			value_type pop_front()
			{
				int result;
				TioDataConverter<value_type> value;

				result = this->container_manager()->container_pop_front(
					this->container_, 
					nullptr,
					value.outptr(),
					nullptr);

				ThrowOnTioClientError(result);

				return value.value();
			}
		};

		template<typename TKey, typename TValue, typename TMetadata=std::string>
		class map : public TioContainerImpl<TKey, TValue, TMetadata, map<TKey, TValue, TMetadata> >
		{
		private:
			map(const map<TKey,TValue, TMetadata> &){}
			map<TKey,TValue, TMetadata>& operator=(const map<TKey,TValue, TMetadata> &){return *this;}
		public:
			typedef map<TKey, TValue, TMetadata> this_type;
			map(){}
		};
	}
}
