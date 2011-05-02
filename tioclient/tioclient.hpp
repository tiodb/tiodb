#pragma once
#include "tioclient.h"
#include <string>

namespace tio
{
	using std::string;
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
		tiodata_set_string(tiodata, v.c_str());
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
			throw std::runtime_error("error");
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
			FromTioData(&tiodata_, &v);
			return v;
		}
	};



	class Connection
	{
		TIO_CONNECTION* connection_;

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

		TIO_CONNECTION* cnptr()
		{
			return connection_;
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
			return value_() == v.value();
		}
	};

	template<typename TKey, typename TValue, typename TMetadata, typename SelfT>
	class TioContainerImpl
	{
	public:
		typedef TKey key_type;
		typedef TValue value_type;
		typedef TMetadata metadata_type;
		typedef ServerValue<SelfT, TKey, TValue> server_value_type;
	protected:
		TIO_CONTAINER* container_;

	public:
		TioContainerImpl() : container_(NULL)
		{
		}

		~TioContainerImpl()
		{
		}

		TIO_CONTAINER* handle()
		{
			return container_;
		}

		void open(Connection* connection, const string& name)
		{
			int result;

			result = tio_open(connection->cnptr(), name.c_str(), NULL, &container_);

			ThrowOnTioClientError(result);
		}

		void create(Connection* connection, const string& name, const string& type)
		{
			int result;

			result = tio_create(connection->cnptr(), name.c_str(), type.c_str(), &container_);

			ThrowOnTioClientError(result);
		}

		void insert(const key_type& key, const value_type& value)
		{
			int result;

			result = tio_container_insert(
				container_, 
				TioDataConverter<key_type>(key).inptr(),
				TioDataConverter<value_type>(value).inptr(),
				NULL);

			ThrowOnTioClientError(result);
		}

		void set(const key_type& key, const value_type& value)
		{
			int result;

			result = tio_container_set(
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

			result = tio_container_get(
				container_, 
				TioDataConverter<key_type>(index).inptr(),
				NULL,
				value.outptr(),
				NULL);

			ThrowOnTioClientError(result);

			return value.value();
		}

		server_value_type operator[](const key_type& key)
		{
			return server_value_type(*static_cast<SelfT*>(this), key);
		}

		void clear()
		{
			int result;

			result = tio_container_clear(container_);

			ThrowOnTioClientError(result);
		}

		size_t size()
		{
			int result, count;

			result = tio_container_get_count(
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
	
	public:
		void push_back(const value_type& value)
		{
			int result;

			result = tio_container_push_back(
				container_, 
				NULL,
				TioDataConverter<TValue>(value).inptr(),
				NULL);

			ThrowOnTioClientError(result);
		}

		void push_front(const value_type& value)
		{
			int result;

			result = tio_container_push_front(
				container_, 
				NULL,
				TioDataConverter<TValue>(value).inptr(),
				NULL);

			ThrowOnTioClientError(result);
		}

		value_type pop_back()
		{
			int result;
			TioDataConverter<value_type> value;

			result = tio_container_pop_back(
				container_, 
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

			result = tio_container_pop_front(
				container_, 
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
	public:
		typedef map<TKey, TValue, TMetadata> this_type;
	};

}