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
#pragma once

//
// TODO: find a better place to this
//
namespace tio
{
	using std::numeric_limits;
	using std::runtime_error;
	using std::map;
	using std::string;
	using std::vector;
	using std::pair;
	using boost::function;
	using boost::shared_ptr;

	template<typename T1, typename T2>
	class __PairAssignDetail__
	{
		T1& t1_;
		T2& t2_;
	public:
		__PairAssignDetail__(T1& t1, T2& t2):
		  t1_(t1), t2_(t2)
		  {}

		  const __PairAssignDetail__<T1, T2>& operator=(const std::pair<T1, T2>& p)
		  {
			  t1_ = p.first;
			  t2_ = p.second;

			  return *this;
		  }
	};

	//
	// this class is used to emulate a tuple/pair attribution, like python.
	// instead of
	// 
	// int a, b;
	// pair<int, int> p = returns_a_pair();
	// a = p.first;
	// b = p.second;
	//
	// you can use
	//
	// int a, b;
	// pair_assign(a,b) = returns_a_pair();
	//
	template<typename T1, typename T2>
	__PairAssignDetail__<T1, T2> 
		pair_assign(T1& t1, T2& t2)
	{
		return __PairAssignDetail__<T1, T2>(t1, t2);
	}

	class Timer
	{
		clock_t start;

	public:
		Timer()
		{
			Start();
		}

		void Start()
		{
			start = clock();
		}

		unsigned int Elapsed()
		{
			return ((clock() - start) * 1000) / CLOCKS_PER_SEC;
		}
	};

}

namespace tio
{
	
	using boost::shared_ptr;

	class TioData
	{
	public:
		enum Type
		{
			Int, Double, Sz, Raw, None, Invalid
		};
	private:
		Type type_;
		
		const char* sz_;
		int int_;
		double double_;
		const unsigned char* raw_;
		size_t rawSize_;
		bool bufferOwnership_;
	public:

		TioData()
		{
			type_ = None;
			bufferOwnership_ = false;
		}

		TioData(int i)
		{
			type_ = None;
			Set(i);
		}

		TioData(double d)
		{
			type_ = None;
			Set(d);
		}

		explicit TioData(const TioData* t)
		{
			type_ = None;
			*this = *t;
		}

		TioData(const void* v, size_t size)
		{
			type_ = None;
			Set(v, size, true);
		}

		TioData(const char* sz, bool copy = true)
		{
			type_ = None;
			Set(sz, copy);
		}

		TioData(const string& str)
		{
			type_ = None;
			Set(str);
		}

		~TioData()
		{
			Free();
		}

		TioData(const TioData& data)
		{
			bufferOwnership_ = false;
			type_ = None;

			if(data.GetDataType() == None)
				return;

			CopyFrom(data);
		}

		bool operator!() const
		{
			return Empty();
		}

		operator bool() const
		{
			return type_ != None;
		}

		TioData& operator = (const TioData& data)
		{
			CopyFrom(data);
			return *this;
		}

		void CopyFrom (const TioData& data)
		{
			Free();

			if(data.GetDataType() == None)
				return;

			switch(data.type_)
			{
			case Int:
				int_ = data.int_;
				break;
			case Double:
				double_ = data.double_;
				break;
			case Sz:
				Set(data.AsSz(), true);
				break;
			case Raw:
				Set(data.AsRaw(), data.GetRawSize(), true);
			default:
				Free();
			}
			
			type_ = data.type_;	
		}

		bool Empty() const
		{
			return type_ == None;
		}

		size_t GetSerializedSize() const
		{
			size_t needSize;
			switch(type_)
			{
			case TioData::Double:
				needSize = sizeof(AsDouble());
				break;

			case TioData::Int:
				needSize = sizeof(AsInt());
				break;

			case TioData::Sz:
				needSize = strlen(AsSz());
				break;

			case TioData::Raw:
				return GetRawSize();

			default:
				//
				// let AsRaw throw the exception
				//
				AsRaw();
				return 69;
			}

			needSize += sizeof(unsigned int) * 2; // size + type

			return needSize;
		}

		static const size_t SERIALIZE_HEADER_SIZE = sizeof(unsigned int) * 2;

		size_t Serialize(void* buffer, size_t bufferSize) const
		{
			size_t needSize = GetSerializedSize();

			if(needSize > bufferSize)
				throw std::invalid_argument("buffer to small");

			unsigned int* data = (unsigned int*)buffer;

			size_t rawSize = GetRawSize();

			data[0] = static_cast<unsigned int>(GetDataType());
			data[1] = static_cast<unsigned int>(rawSize);

			memcpy(&data[2], AsRaw(), rawSize);

			return needSize;
		}

		size_t Deserialize(const void* buffer, size_t bufferSize)
		{
			if(bufferSize == 0)
			{
				Free();
				return 0;
			}

			unsigned int* data = (unsigned int*)buffer;

			Type type = static_cast<Type>(data[0]);
			unsigned int size = data[1];
			
			if(bufferSize < size + SERIALIZE_HEADER_SIZE)
				throw std::invalid_argument("incomplete buffer");

			Free();

			switch(type)
			{
				case TioData::Sz:
					this->type_ = TioData::Sz;
					this->sz_ = new char[size + 1];
					this->bufferOwnership_ = true;
					
					memcpy((void*)this->sz_, &data[2], size);
					((char*)this->sz_)[size] = '\0';

					break;

				case TioData::Int:
					if(size != sizeof(int))
						throw std::invalid_argument("invalid data size to int data type");

					this->int_ = static_cast<int>(data[2]);
					this->type_ = TioData::Int;

					break;
				
				case TioData::Double:
					if(size != sizeof(double))
						throw std::invalid_argument("invalid data size to double data type");

					this->double_ = *reinterpret_cast<double*>(&data[2]);
					this->type_ = TioData::Double;

					break;

				case TioData::Raw:
					this->type_ = TioData::Raw;
					this->raw_ = new unsigned char[size];
					this->bufferOwnership_ = true;

					memcpy((void*)this->raw_, &data[2], size);

					break;
				
				default:
					throw std::invalid_argument("invalid data type");

			}

			return size + SERIALIZE_HEADER_SIZE;
		}

		bool operator==(const TioData& rs)
		{
			if(type_ == None)
			{
				if(rs.type_ == None)
					return true;
				else
					return false;
			}
			else if(rs.type_ == None)
			{
				return false;
			}

			return type_ == rs.type_ && 
				   GetRawSize() == rs.GetRawSize() &&
				   memcmp(AsRaw(), rs.AsRaw(), GetRawSize()) == 0;
		}

		void Clear()
		{
			Free();
		}

		inline void Free()
		{
			if(type_ == None)
				return;

			if(bufferOwnership_)
			{
				if(type_ == Sz)
				{
					delete sz_;
					sz_ = 0;
				}
				else if(type_ == Raw)
				{
					delete raw_;
					sz_ = 0;
				}
				
				bufferOwnership_ = false;
			}

			type_ = None;
		}

		void Set(const TioData& v)
		{
			CopyFrom(v);
		}

		void Set(int v)
		{
			Free();
			type_ = Int;
			int_ = v;
		}

		void Set(double v)
		{
			Free();
			type_ = Double;
			double_ = v;
		}

		void Set(const char* v, bool copy)
		{
			Free();
			if(copy)
			{
				size_t len = strlen(v) + 1;
				sz_ = new char[len];
				memcpy(const_cast<char*>(sz_), v, len);
			}
			else
				sz_ = v;

			bufferOwnership_ = copy;
			
			type_ = Sz;
		}

		void Set(const string& str)
		{
			Set(str.c_str(), true);
		}

		void Set(const void* v, size_t size, bool copy)
		{
			Free();

			if(copy)
			{
				raw_ = new unsigned char[size];
				memcpy((void*)raw_, v, size);
				bufferOwnership_ = true;
			}
			else
				raw_ = (const unsigned char*)v;
			
			type_ = Raw;
			rawSize_ = size;
		}

		void CheckDataType(Type t) const 
		{
			if(type_ != t)
				throw std::runtime_error("wrong data type");
		}

		bool IsNull() const
		{
			return GetDataType() == TioData::None;
		}

		Type GetDataType() const 
		{
			return type_;
		}

		int AsInt() const 
		{
			CheckDataType(Int);
			return int_;
		}

		double AsDouble() const 
		{
			CheckDataType(Double);
			return double_;
		}

		const char* AsSz() const 
		{
			CheckDataType(Sz);
			return sz_;
		}

		const void* AsRaw() const 
		{
			switch(type_)
			{
				case Int : return const_cast<const int*>(&int_);
				case Double : return &double_;
				case Sz : return sz_;
				case Raw: return raw_;
			}

			throw std::runtime_error("wrong data type");
		}

		size_t GetRawSize() const
		{
			switch(type_)
			{
				case Int : return sizeof(int);
				case Double : return sizeof(double);
				case Sz : return strlen(sz_);
				case Raw: return rawSize_;
			}
			
			throw std::runtime_error("wrong data type");
		}


	};

	inline std::ostream& operator << (std::ostream& stream, const TioData& td)
	{
		switch(td.GetDataType())
		{
		case TioData::Int:
			stream << td.AsInt();
			break;
		case TioData::Sz:
			stream << td.AsSz();
			break;
		case TioData::Double:
			stream << td.AsDouble();
		    break;
		case TioData::Raw:
			stream.rdbuf()->sputn(
				reinterpret_cast<const char*>(td.AsRaw()), 
				static_cast<std::streamsize>(td.GetRawSize()));
		    break;
		case TioData::None:
			stream << "**NULL**";
			break;
		default:
		    throw std::runtime_error("invalid data type");
		}

		return stream;
	}

	template<typename T>
	T tio_cast(const TioData& data)
	{

	}



	inline std::ostream& operator << (std::ostream& stream, const TioData* td)
	{
		if(td)
			stream << *td;
		else 
			stream << "NULL";

		return stream;
	}

	inline string GetDataTypeAsString(const TioData& data)
	{
		switch(data.GetDataType())
		{
		case TioData::Int: return "int";
		case TioData::Raw: return "raw";
		case TioData::Double: return "double";
		case TioData::Sz: return "string";
		}

		return "INTERNAL_ERROR";
	}

	//
	// support for negative indexes, just like python. -1 is the last,
	// -2 is the one before the last, and so on...
	//
	inline unsigned int NormalizeIndex(int index, int size, bool checkBounds = true)
	{	
		if(index < 0)
		{
			index = abs(index);

			if(checkBounds && index > size)
				throw std::invalid_argument("out of bounds");

			index = size - index;
		}
		else
		{
			if(checkBounds && index >= size)
				throw std::invalid_argument("out of bounds");
		}

		ASSERT(index >=0);

		return static_cast<unsigned int>(index);
	}

	//
	// if the index is outside bound, will bring it to the nearest bound
	//
	inline int NormalizeForQueries(int index, int size)
	{
		if(size == 0 || (index < 0 && abs(index) > size))
			return 0;
		else if(index > size)
			return size;
		else
			return NormalizeIndex(index, size);
	}

	inline void NormalizeQueryLimits(int* start, int* end, int containerSize)
	{
		*start = NormalizeForQueries(*start, containerSize);

		if(*end == 0)
			*end = containerSize;
		else
			*end = NormalizeForQueries(*end, containerSize);

		if(*start > *end)
			*start = *end;
	}

	typedef boost::function<void(const string&, const TioData&, const TioData&, const TioData&)> EventSink;

	static const TioData TIONULL = TioData();

	INTERFACE ITioResultSet
	{
		virtual bool GetRecord(TioData* key, TioData* value, TioData* metadata) = 0;

		virtual bool MoveNext() = 0;
		virtual bool MovePrevious() = 0;

		virtual bool AtBegin() = 0;
		virtual bool AtEnd() = 0;

		virtual unsigned int RecordCount() = 0;

		virtual TioData Source() = 0;
	};


	INTERFACE ITioStorage
	{
		virtual size_t GetRecordCount() = 0;
		
		virtual void PushBack(const TioData& key, const TioData& value, const TioData& metadata) = 0;
		virtual void PushFront(const TioData& key, const TioData& value, const TioData& metadata) = 0;

		virtual void PopBack(TioData* key, TioData* value, TioData* metadata) = 0;
		virtual void PopFront(TioData* key, TioData* value, TioData* metadata) = 0;

		virtual void GetRecord(const TioData& searchKey, TioData* key,  TioData* value, TioData* metadata) = 0;

		virtual void Set(const TioData& key, const TioData& value, const TioData& metadata) = 0;
		virtual void Insert(const TioData& key, const TioData& value, const TioData& metadata) = 0;
		virtual void Delete(const TioData& key, const TioData& value, const TioData& metadata) = 0;

		virtual shared_ptr<ITioResultSet> Query(int startOffset, int endOffset, const TioData& query) = 0;

		virtual void Clear() = 0;

		virtual string GetType() = 0;
		virtual string GetName() = 0;

		virtual string Command(const string& command) = 0;

		virtual unsigned int Subscribe(EventSink sink, const string& start) = 0;
		virtual void Unsubscribe(unsigned int cookie) = 0;
	};

	INTERFACE ITioPropertyMap
	{
		virtual string Get(const string& key) = 0;
		virtual void Set(const string& key, const string& value) = 0;
	};

	struct StorageInfo
	{
		string name;
		string type;
	};

	INTERFACE ITioStorageManager
	{
		virtual bool Exists(const string& containerType, const string& containerName) = 0;

		virtual pair<shared_ptr<ITioStorage>, shared_ptr<ITioPropertyMap> > 
			OpenStorage(const string& type, const string& name) = 0;

		virtual pair<shared_ptr<ITioStorage>, shared_ptr<ITioPropertyMap> > 
			CreateStorage(const string& type, const string& name) = 0;

		virtual void DeleteStorage(const string& type, const string& name) = 0;
	
		virtual std::vector<string> GetSupportedTypes() = 0;

		virtual std::vector<StorageInfo> GetStorageList() = 0;
	};

	INTERFACE ITioContainer
	{
		virtual string GetName() = 0;
		virtual size_t GetRecordCount() = 0;

		virtual string Command(const string& command) = 0;

		virtual void GetRecord(const TioData& searchKey, TioData* key, TioData* value, TioData* metadata = NULL) = 0;

		virtual void PushBack(const TioData& key, const TioData& value, const TioData& metadata = TIONULL) = 0;
		virtual void PushFront(const TioData& key, const TioData& value, const TioData& metadata = TIONULL) = 0;

		virtual void PopBack(TioData* key, TioData* value, TioData* metadata = NULL) = 0;
		virtual void PopFront(TioData* key, TioData* value, TioData* metadata = NULL) = 0;

		virtual void Insert(const TioData& key, const TioData& value, const TioData& metadata = TIONULL) = 0;
		virtual void Set(const TioData& key, const TioData& value, const TioData& metadata = TIONULL) = 0;
		virtual void Delete(const TioData& key, const TioData& value = TIONULL, const TioData& metadata = TIONULL) = 0;

		virtual shared_ptr<ITioResultSet> Query(int startOffset, int endOffset, const TioData& query) = 0;

		virtual void Clear() = 0;

		virtual void SetProperty(const string& key, const string& value) = 0;
 		virtual string GetProperty(const string& key) = 0;

		virtual unsigned int Subscribe(EventSink sink, const string& start) = 0;
		virtual void Unsubscribe(unsigned int cookie) = 0;

		virtual string GetType() = 0;
	};

	//
	// multiplexes events to several sinks
	//
	class EventDispatcher
	{
		typedef map<unsigned int, EventSink> SinkMap;
		SinkMap sinks_;
		unsigned int lastCookie_;
	public:

		EventDispatcher()
			: lastCookie_(0)
		{

		}
		unsigned int Subscribe(EventSink sink)
		{
			sinks_[++lastCookie_] = sink;
			return lastCookie_;
		}

		void Unsubscribe(unsigned int cookie)
		{
			sinks_.erase(cookie);
		}

		void RaiseEvent(const string& eventName, const TioData& key, const TioData& value, const TioData& metadata)
		{
			for(SinkMap::iterator i = sinks_.begin() ; i != sinks_.end() ; ++i)
			{
				EventSink& sink = i->second;
				sink(eventName, key, value, metadata);
			}
		}
	};


	class Container : 
		public ITioContainer,
		boost::noncopyable
	{
		shared_ptr<ITioPropertyMap> propertyMap_;
		shared_ptr<ITioStorage> storage_;
		tio::recursive_mutex mutex_;

		inline size_t GetRealRecordNumber(int recNumber)
		{
			if(recNumber >= 0)
				if(recNumber > static_cast<int>(GetRecordCount()) - 1)
					throw std::out_of_range("invalid record number");
				else
					return recNumber;

			int realRecNumber = static_cast<int>(GetRecordCount()) + recNumber;

			if(realRecNumber < 0)
				throw std::out_of_range("invalid record number");

			return realRecNumber;
		}


	public:

		Container(shared_ptr<ITioStorage> storage, shared_ptr<ITioPropertyMap> propertyMap) :
			storage_(storage),
			propertyMap_(propertyMap)
		{}
		
		~Container()
		{
			return;
		}
		
		virtual string GetName()
		{
			tio::recursive_mutex::scoped_lock lock(mutex_);
			return storage_->GetName();
		}

		virtual string GetType()
		{
			tio::recursive_mutex::scoped_lock lock(mutex_);
			return storage_->GetType();
		}

		virtual size_t GetRecordCount()
		{
			tio::recursive_mutex::scoped_lock lock(mutex_);
			return storage_->GetRecordCount();
		}

		virtual void GetRecord(const TioData& searchKey, TioData* key,  TioData* value, TioData* metadata)
		{
			tio::recursive_mutex::scoped_lock lock(mutex_);
			storage_->GetRecord(searchKey, key, value, metadata);
		}

		virtual void PopBack(TioData* key, TioData* value, TioData* metadata)
		{
			tio::recursive_mutex::scoped_lock lock(mutex_);
			storage_->PopBack(key, value, metadata);
		}

		virtual void PopFront(TioData* key, TioData* value, TioData* metadata)
		{
			tio::recursive_mutex::scoped_lock lock(mutex_);
			storage_->PopFront(key, value, metadata);
		}

				
		virtual void PushBack(const TioData& key, const TioData& value, const TioData& metadata)
		{
			tio::recursive_mutex::scoped_lock lock(mutex_);
			storage_->PushBack(key, value, metadata);
		}

		virtual void PushFront(const TioData& key, const TioData& value, const TioData& metadata)
		{
			tio::recursive_mutex::scoped_lock lock(mutex_);
			storage_->PushFront(key, value, metadata);
		}

		
		virtual void Insert(const TioData& key, const TioData& value, const TioData& metadata)
		{
			tio::recursive_mutex::scoped_lock lock(mutex_);
			storage_->Insert(key, value, metadata);
		}

		virtual void Set(const TioData& key, const TioData& value, const TioData& metadata)
		{
			tio::recursive_mutex::scoped_lock lock(mutex_);
			storage_->Set(key, value, metadata);
		}

		virtual void Delete(const TioData& key, const TioData& value, const TioData& metadata)
		{
			tio::recursive_mutex::scoped_lock lock(mutex_);
			storage_->Delete(key, value, metadata);
		}

		virtual shared_ptr<ITioResultSet> Query(int startOffset, int endOffset, const TioData& query)
		{
			tio::recursive_mutex::scoped_lock lock(mutex_);
			return storage_->Query(startOffset, endOffset, query);
		}

		virtual void Clear()
		{
			tio::recursive_mutex::scoped_lock lock(mutex_);
			storage_->Clear();
		}

		virtual string Command(const string& command)
		{
			tio::recursive_mutex::scoped_lock lock(mutex_);
			return storage_->Command(command);
		}

		virtual void SetProperty(const string& key, const string& value)
		{
			tio::recursive_mutex::scoped_lock lock(mutex_);
			propertyMap_->Set(key, value);
		}

		virtual string GetProperty(const string& key)
		{
			tio::recursive_mutex::scoped_lock lock(mutex_);
			return propertyMap_->Get(key);
		}

		virtual unsigned int Subscribe(EventSink sink, const string& start)
		{
			tio::recursive_mutex::scoped_lock lock(mutex_);
			return storage_->Subscribe(sink, start);
		}
		virtual void Unsubscribe(unsigned int cookie)
		{
			tio::recursive_mutex::scoped_lock lock(mutex_);
			storage_->Unsubscribe(cookie);
		}
	};

	struct ValueAndMetadata
	{
		ValueAndMetadata() 
		{}


		ValueAndMetadata(const TioData& value, const TioData& metadata) 
			: value(value), metadata(metadata)
		{
			
		}

		TioData value;
		TioData metadata;
	};

	class ContainerRecord
	{
		string schema_;
		vector<string> fields_;

		typedef map<string, size_t> FieldsNameMap;
		FieldsNameMap fieldsNames_;
		string separators_;

	public:

		ContainerRecord(const string& schema):
			separators_("^")
		{
			SetSchema(schema);
		}

		ContainerRecord(shared_ptr<ITioContainer> container):
			separators_("^")
		{
			SetSchema(container->GetProperty("schema"));
		}

		ContainerRecord(shared_ptr<ITioContainer> container, const string& key):
			separators_("^")
		{
			SetSchema(container->GetProperty("schema"));
			
			TioData	value;
			container->GetRecord(key, NULL, &value, NULL);
			SetRecord(value.AsSz());
		}

		void SetSchema(const string& schema)
		{
			vector<string> names;
			boost::algorithm::split(names, schema, boost::algorithm::is_any_of(separators_));

			size_t size = names.size();

			for(size_t a = 0 ; a < size ; a++)
				fieldsNames_[names[a]] = a;
		}

		void SetRecord(const string& record)
		{
			boost::algorithm::split(fields_, record, boost::algorithm::is_any_of(separators_));
		}

		size_t GetFieldCount() const
		{
			return fields_.size();
		}

		string GetField(size_t index) const 
		{
			if(index + 1 > fields_.size())
				return string();

			return fields_[index];
		}

		string GetField(const string& fieldName) const
		{
			FieldsNameMap::const_iterator i = fieldsNames_.find(fieldName);

			if(i == fieldsNames_.end())
				return string();

			size_t index = i->second;

			return GetField(index);
		}
	};



	template<class ContainerT>
	bool NumericIndexBasedContainerGetter(
		size_t currentQueryIndex, typename ContainerT::const_iterator current, TioData* key, TioData* value, TioData* metadata)
	{
		//
		// TODO: 64bits problem here, size_t is bigger than int
		//
		if(key)
			key->Set((int)currentQueryIndex);

		if(value)
			*value = current->value;

		if(metadata)
			*metadata = current->metadata;

		return true;
	}

	template<class ContainerT>
	bool MapContainerGetter(
		size_t currentQueryIndex, typename ContainerT::const_iterator current, TioData* key, TioData* value, TioData* metadata)
	{
		if(key)
			key->Set(current->first);

		if(value)
			value->Set(current->second.value);

		if(metadata)
			metadata->Set(current->second.metadata);

		return true;
	}

	template<class ContainerT>
	class StlContainerResultSet : public ITioResultSet
	{
		typedef function<bool (size_t, typename ContainerT::const_iterator, TioData*, TioData*, TioData*)> ItemGetFunction;

		ItemGetFunction itemGetFunction_;
		TioData source_;

		typename ContainerT::const_iterator begin_, current_, end_;

		size_t currentIndex_;
		size_t recordCount_;
		bool invalidated_;

	public:

		StlContainerResultSet(
			const TioData& source,
			unsigned int beginIndex, 
			typename ContainerT::const_iterator begin, 
			typename ContainerT::const_iterator end,
			typename ContainerT::size_type recordCount = numeric_limits<typename ContainerT::size_type>::max(),
			ItemGetFunction itemGetFunction = &NumericIndexBasedContainerGetter<ContainerT>
			)

			: source_(source), current_(begin), begin_(begin), end_(end), 
			currentIndex_(beginIndex), itemGetFunction_(itemGetFunction), recordCount_(recordCount)
		{
			invalidated_ = false;
		}

		void Invalidate()
		{
			invalidated_ = true;
		}

		void CheckInvalidated()
		{
			if(invalidated_)
				throw runtime_error("invalidated query");
		}

		virtual bool GetRecord(TioData* key, TioData* value, TioData* metadata)
		{
			CheckInvalidated();

			if(current_ == end_)
				return false;

			return itemGetFunction_(currentIndex_, current_, key, value, metadata);
		}

		virtual bool MoveNext()
		{
			CheckInvalidated();

			if(current_ == end_)
				return false;

			++current_;
			++currentIndex_;

			if(current_ == end_)
				return false;

			return true;
		}

		virtual bool MovePrevious()
		{
			CheckInvalidated();

			if(current_ == begin_)
				return false;

			--current_;
			--currentIndex_;

			return true;
		}

		virtual bool AtBegin()
		{
			CheckInvalidated();

			return current_ == begin_;
		}

		virtual bool AtEnd()
		{
			CheckInvalidated();

			return current_ == end_;
		}

		virtual TioData Source()
		{
			CheckInvalidated();

			return source_;
		}

		virtual unsigned RecordCount()
		{
			CheckInvalidated();

			return recordCount_;
		}
	};






















	template<class ContainerT>
	class GenericResultSet : public ITioResultSet
	{
		TioData source_;
		shared_ptr<ContainerT> container_;

		unsigned int currentIndex_, beginIndex_, endIndex_;
		unsigned int recordCount_;
		bool invalidated_;

	public:
		GenericResultSet(
			const TioData& source, 
			shared_ptr<ContainerT> container,
			unsigned int beginIndex,
			unsigned int endIndex
			)
			: source_(source), currentIndex_(beginIndex), beginIndex_(beginIndex), endIndex_(endIndex), 
			  recordCount_(endIndex - beginIndex),
			  container_(container)
		{
			invalidated_ = false;
		}

		void Invalidate()
		{
			invalidated_ = true;
		}

		void CheckInvalidated()
		{
			if(invalidated_)
				throw runtime_error("invalidated query");
		}

		virtual bool GetRecord(TioData* key, TioData* value, TioData* metadata)
		{
			CheckInvalidated();

			if(currentIndex_ == endIndex_)
				return false;

			container_->GetRecord((int)currentIndex_, key, value, metadata);

			return true;
		}

		virtual bool MoveNext()
		{
			CheckInvalidated();

			if(currentIndex_ == endIndex_)
				return false;

			++currentIndex_;

			if(currentIndex_ == endIndex_)
				return false;

			return true;
		}

		virtual bool MovePrevious()
		{
			CheckInvalidated();

			if(currentIndex_ == beginIndex_)
				return false;

			--currentIndex_;

			return true;
		}

		virtual bool AtBegin()
		{
			CheckInvalidated();

			return currentIndex_ == beginIndex_;
		}

		virtual bool AtEnd()
		{
			CheckInvalidated();

			return currentIndex_ == endIndex_;
		}

		virtual TioData Source()
		{
			CheckInvalidated();

			return source_;
		}

		virtual unsigned int RecordCount()
		{
			CheckInvalidated();

			return recordCount_;
		}
	};

	inline bool IsListContainer(shared_ptr<ITioContainer> container)
	{
		string type = container->GetType();
		return type == "volatile_list" || type == "persistent_list" ||
			   type == "volatile_vector" || type == "persistent_vector";
	}

	inline bool IsMapContainer(shared_ptr<ITioContainer> container)
	{
		string type = container->GetType();
		return type == "volatile_map" || type == "persistent_map";
	}

}
