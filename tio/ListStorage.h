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
#include "Container.h"

namespace tio {
namespace MemoryStorage
{

	using std::string;
	using std::vector;
	using std::map;
	using std::list;


typedef list<ValueAndMetadata> ListType;

class ListStorage : 
	boost::noncopyable,
	public ITioStorage
{
private:

	ListType data_;
	string name_, type_;
	EventDispatcher dispatcher_;

public:

	ListStorage(const string& name, const string& type) :
		name_(name),
		type_(type)
	{}

	  virtual string GetName()
	  {
		  return name_;
	  }

	  virtual string GetType()
	  {
		  return type_;
	  }

	  virtual string Command(const string& command)
	  {
		  throw std::invalid_argument("\"command\" not supported by the container");
	  }

	  virtual size_t GetRecordCount()
	  {
		  return data_.size();
	  }

	  virtual void PushBack(const TioData& key, const TioData& value, const TioData& metadata)
	  {
		  CheckValue(value);
		  
		  data_.push_back(ValueAndMetadata(value, metadata));

		  dispatcher_.RaiseEvent("push_back", static_cast<int>(data_.size() - 1), value, metadata);
	  }

	  virtual void PushFront(const TioData& key, const TioData& value, const TioData& metadata)
	  {
		  CheckValue(value);
		  data_.push_front(ValueAndMetadata(value, metadata));

		  dispatcher_.RaiseEvent("push_front", key, value, metadata);
	  }

	virtual void PopBack(TioData* key, TioData* value, TioData* metadata)
	{
		if(data_.empty())
			throw std::invalid_argument("empty");

		ValueAndMetadata& data = data_.back();

		if(value)
			*value = data.value;

		if(metadata)
			*metadata = data.metadata;

		data_.pop_back();

		dispatcher_.RaiseEvent("pop_back",
			key ? *key : TIONULL, 
			value ? *value : TIONULL,
			metadata ? *metadata : TIONULL);
	}

	virtual void PopFront(TioData* key, TioData* value, TioData* metadata)
	{
		if(data_.empty())
			throw std::invalid_argument("empty");

		ValueAndMetadata& data = data_.front();

		if(value)
			*value = data.value;

		if(metadata)
			*metadata = data.metadata;

		data_.pop_front();

		dispatcher_.RaiseEvent("pop_front", 
			key ? *key : TIONULL, 
			value ? *value : TIONULL,
			metadata ? *metadata : TIONULL);
	}

	void CheckValue(const TioData& value)
	{
		if(!value)
			throw std::invalid_argument("value??");
	}

	ListType::iterator GetOffset(const TioData& key, bool canBeTheEnd = false)
	{
		int index = NormalizeIndex(key.AsInt(), data_.size());
		ListType::iterator i;

		//
		// advance a list iterator is expensive. If it's near the end, will
		// walk backwards
		//
		if(index <= static_cast<int>(data_.size() / 2))
		{
			i = data_.begin();
			for(int x = 0  ; x < index ; ++x, ++i)
				;
		}
		else
		{
			i = data_.end();

			int walk = data_.size() - index;

			for(int x = 0  ; x < walk ; ++x, --i)
				;
		}

		return i;
	}

	virtual void Set(const TioData& key, const TioData& value, const TioData& metadata)
	{
		ListType::iterator i = GetOffset(key);
		
		ValueAndMetadata& valueAndMetadata = *i;

		if(value)
			valueAndMetadata.value = value;

		if(metadata)
			valueAndMetadata.metadata = metadata;

		dispatcher_.RaiseEvent("set", key, value, metadata); 
	}

	virtual void Insert(const TioData& key, const TioData& value, const TioData& metadata)
	{
		size_t index = key.AsInt();

		if(index == 0)
			data_.push_front(ValueAndMetadata(value, metadata));
		else if (index == data_.size())
			data_.push_back(ValueAndMetadata(value, metadata));
		else
		{
			ListType::iterator i = GetOffset(key);
			data_.insert(i, ValueAndMetadata(value, metadata));
		}

		dispatcher_.RaiseEvent("insert", key, value, metadata); 
	}

	virtual void Delete(const TioData& key, const TioData& value, const TioData& metadata)
	{
		ListType::iterator i = GetOffset(key);

		data_.erase(i);

		dispatcher_.RaiseEvent("delete", key, value, metadata); 
	}

	virtual void Clear()
	{
		data_.clear();

		dispatcher_.RaiseEvent("clear", TIONULL, TIONULL, TIONULL); 
	}

	virtual shared_ptr<ITioResultSet> Query(int startOffset, int endOffset, const TioData& query)
	{
		if(!query.IsNull())
			throw std::runtime_error("query type not supported by this container");

		ListType::const_iterator begin, end;

		//
		// if client is asking for a negative index that's bigger than the container,
		// will start from beginning. Ex: if container size is 3 and start = -5, will start from 0
		//
		if(GetRecordCount() == 0)
		{
			begin = end = data_.end();
			startOffset = 0;
		}
		else
		{
			int recordCount = GetRecordCount();

			NormalizeQueryLimits(&startOffset, &endOffset, recordCount);
			
			if(startOffset == 0)
				begin = data_.begin();
			else if(startOffset == recordCount)
				begin = data_.end();
			else
				begin = GetOffset(startOffset);

			if(endOffset == 0)
				end = data_.begin();
			else if(endOffset == recordCount)
				end = data_.end();
			else
				end = GetOffset(endOffset);	
		}

		return shared_ptr<ITioResultSet>(
			new StlContainerResultSet<ListType>(TIONULL, startOffset, begin, end));
	}

	virtual unsigned int Subscribe(EventSink sink, const string& start)
	{
		unsigned int cookie = 0;
		int startIndex = 0;
		ListType::const_iterator i;

		if(!start.empty())
		{
			try
			{
				startIndex = lexical_cast<int>(start);
			}
			catch(std::exception&)
			{
				throw std::invalid_argument("invalid start index");
			}
		}

		if(start.empty() || startIndex == 0 && data_.size() == 0)
			return dispatcher_.Subscribe(sink);

		i =  GetOffset(startIndex);

		cookie = dispatcher_.Subscribe(sink);

		//
		// key is the start index to send
		//
		for( ; i != data_.end() ; ++i)
		{
			const ValueAndMetadata& data = *i;
			sink("push_back", TIONULL, data.value, data.metadata);
		}

		return cookie;

	}
	virtual void Unsubscribe(unsigned int cookie)
	{
		dispatcher_.Unsubscribe(cookie);
	}

	virtual void GetRecord(const TioData& searchKey, TioData* key,  TioData* value, TioData* metadata)
	{
		ListType::iterator i = GetOffset(searchKey);

		if(key)
			*key = searchKey;

		if(value)
			*value = i->value;

		if(metadata)
			*metadata = i->metadata;

	}
};

}}
