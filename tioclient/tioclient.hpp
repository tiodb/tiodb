#pragma once
#include "tioclient.h"
#include <string>
#include <sstream>

namespace tio
{
	using std::string;
	using std::stringstream;
	using std::runtime_error;

	inline void ToTioData(int v, TIO_DATA* tiodata)
	{
		tiodata_set_as_none(tiodata);
		tiodata_set_int(tiodata, v);
	}

	//
	// TODO: tio doesnt support unsigned int. But STL containers indexes
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

	inline void FromTioData(const TIO_DATA* tiodata, string* value)
	{
		if(tiodata->data_type != TIO_DATA_TYPE_STRING)
			throw runtime_error("wrong data type");

		*value = tiodata->string_;
	}

	inline void ThrowOnTioClientError(int result)
	{
		//
		// TODO: create a typed exception an fill it accordingly
		// 
		if(result < 0)
		{
			stringstream str;
			str << "client error " << result;
			throw std::runtime_error(str.str());
		}
	}

	template<typename TValue>
	class TioDataConverter
	{
		TIO_DATA tiodata_;
	public:

		TioDataConverter()
		{
			tiodata_init(&tiodata_);
		}

		explicit TioDataConverter(const TValue& v)
		{
			tiodata_init(&tiodata_);
			ToTioData(v, &tiodata_);
		}

		const TIO_DATA* inptr()
		{
			return &tiodata_;
		}

		TIO_DATA* outptr()
		{
			tiodata_set_as_none(&tiodata_);
			return &tiodata_;
		}

		TValue value()
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
		virtual bool connected()=0;
	};



	class Connection : private IContainerManager
	{
		TIO_CONNECTION* connection_;

	protected:
		virtual int create(const char* name, const char* type, void** handle)
		{
			return tio_create(connection_, name, type, (TIO_CONTAINER**)handle);
		}

		virtual int open(const char* name, const char* type, void** handle)
		{
			return tio_open(connection_, name, type, (TIO_CONTAINER**)handle);
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
			return tio_container_query((TIO_CONTAINER*)handle, start, end, query_callback, cookie);
		}

		virtual int container_subscribe(void* handle, struct TIO_DATA* start, event_callback_t event_callback, void* cookie)
		{
			return tio_container_subscribe((TIO_CONTAINER*)handle, start, event_callback, cookie);
		}

		virtual int container_unsubscribe(void* handle)
		{
			return tio_container_unsubscribe((TIO_CONTAINER*)handle);
		}

	public:
		Connection() : connection_(NULL)
		{
			tio_initialize();
		}

		void Connect(const string& host, short port)
		{
			int result;

			Disconnect();

			result = tio_connect(host.c_str(), port, &connection_);

			ThrowOnTioClientError(result);
		}

		void Disconnect()
		{
			tio_disconnect(connection_);
			connection_ = NULL;
		}

		bool connected()
		{
			return !!cnptr();
		}


		TIO_CONNECTION* cnptr()
		{
			return connection_;
		}

		IContainerManager* container_manager()
		{
			return this;
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
			typedef void (*EventCallbackT)(const string& /*eventName */, const TKey&, const TValue&);

		protected:
			void* container_;
			std::string name_;
			IContainerManager* containerManager_;
			EventCallbackT eventCallback_;

			IContainerManager* container_manager()
			{
				if(!containerManager_)
					throw std::runtime_error("container closed");

				return containerManager_;
			}

		public:
			TioContainerImpl() : 
			    container_(NULL), 
				containerManager_(NULL),
				eventCallback_(NULL)
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

				result = container_manager()->open(name.c_str(), NULL, &container_);

				ThrowOnTioClientError(result);

				name_ = name;
			}

			template<typename TConnection>
			void create(TConnection* cn, const string& name, const string& type)
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

			static void EventCallback(void* cookie, unsigned int /*handle*/, unsigned int /*event_code*/, const struct TIO_DATA* key, const struct TIO_DATA* value, const struct TIO_DATA*)
			{
				this_type* me = (this_type*)cookie;
				TKey typedKey;
				TValue typedValue;

				FromTioData(key, &typedKey);
				FromTioData(value, &typedValue);
				
				me->eventCallback_("event", typedKey, typedValue);
			}

			void subscribe(EventCallbackT callback)
			{
				int result;

				eventCallback_ = callback;

				result = container_manager()->container_subscribe(
					container_,
					NULL,
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
					TioDataConverter<key_type>(key).inptr(),
					value.outptr(),
					NULL);

				ThrowOnTioClientError(result);

				return value.value();
			}

			void insert(const key_type& key, const value_type& value)
			{
				int result;

				result = container_manager()->container_insert(
					container_, 
					TioDataConverter<key_type>(key).inptr(),
					TioDataConverter<value_type>(value).inptr(),
					NULL);

				ThrowOnTioClientError(result);
			}

			void set(const key_type& key, const value_type& value)
			{
				int result;

				result = container_manager()->container_set(
					container_, 
					TioDataConverter<key_type>(key).inptr(),
					TioDataConverter<value_type>(value).inptr(),
					NULL);

				ThrowOnTioClientError(result);
			}

			value_type at(const key_type& index)
			{
				int result;
				TioDataConverter<value_type> value;

				result = container_manager()->container_get(
					container_, 
					TioDataConverter<key_type>(index).inptr(),
					NULL,
					value.outptr(),
					NULL);

				ThrowOnTioClientError(result);

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
		};
	
		template<typename TValue, typename TMetadata=std::string>
		class list : public TioContainerImpl<size_t, TValue, TMetadata, list<TValue, TMetadata> >
		{
		public:
			typedef list<TValue, TMetadata> this_type;
			typedef typename TioContainerImpl<size_t, TValue, TMetadata, list<TValue, TMetadata> >::value_type value_type;

	
		public:
			void push_back(const value_type& value)
			{
				int result;

				result = this->container_manager()->container_push_back(
					this->container_, 
					NULL,
					TioDataConverter<TValue>(value).inptr(),
					NULL);

				ThrowOnTioClientError(result);
			}

			void push_front(const value_type& value)
			{
				int result;

				result = this->container_manager()->container_push_front(
					this->container_, 
					NULL,
					TioDataConverter<TValue>(value).inptr(),
					NULL);

				ThrowOnTioClientError(result);
			}

			value_type pop_back()
			{
				int result;
				TioDataConverter<value_type> value;

				result = this->container_manager()->container_pop_back(
					this->container_, 
					NULL,
					value.outptr(),
					NULL);

				ThrowOnTioClientError(result);

				return value.value();
			}

			value_type pop_front()
			{
				int result;
				TioDataConverter<value_type> value;

				result = this->container_manager()->container_pop_front(
					this->container_, 
					NULL,
					value.outptr(),
					NULL);

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
