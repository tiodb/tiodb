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
using boost::bad_lexical_cast;
using std::distance;

class MapStorage : 
	boost::noncopyable,
	public ITioStorage,
	public ITioPropertyMap
{
private:

	typedef map<string, ValueAndMetadata> DataMap;

	DataMap data_;
	string name_, type_;
	EventDispatcher dispatcher_;

	inline pair<string, ValueAndMetadata> GetInternalRecord(const TioData& key)
	{
		if(key.GetDataType() == TioData::Int)
		{
			int offset = NormalizeIndex(key.AsInt(), data_.size());

			DataMap::iterator i = data_.begin();
			std::advance(i, offset);

			return *i;
		}
		
		DataMap::iterator i = data_.find(key.AsSz());

		if(i == data_.end())
			throw std::invalid_argument("key not found");

		return *i;
	}


public:

	MapStorage(const string& name, const string& type) :
		name_(name),
		type_(type)
	  {}

	  //
	  // ITioPropertyMap
	  //
	  virtual void Set(const string& key, const string& value)
	  {
		  throw std::runtime_error("can't change special property");
	  }
	  virtual string Get(const string& key)
	  {
		  if(key == "__keys__")
		  {
			  if(data_.empty())
				  return string();

			  stringstream buffer;
			  
			  for(DataMap::const_iterator i = data_.begin() ; i != data_.end() ; ++i)
			  {
				  buffer << i->first << "\r\n";
			  }

			  //
			  // delete last \r\n
			  //
			  string str = buffer.str();
			  
			  return string(str.begin(), str.end() - 2);
		  }

		  throw std::invalid_argument("key not found");
	  }

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
		  throw std::invalid_argument("\"command\" not supported");
	  }

	  virtual size_t GetRecordCount()
	  {
		  return data_.size();
	  }

	  virtual void PushBack(const TioData& key, const TioData& value, const TioData& metadata)
	  {
		  throw std::invalid_argument("\"push_back\" not supported by this container");
	  }

	  virtual void PushFront(const TioData& key, const TioData& value, const TioData& metadata)
	  {
		  throw std::invalid_argument("\"push_front\" not supported by this container");
	  }

	  virtual void PopBack(TioData* key, TioData* value, TioData* metadata)
	  {
		  throw std::invalid_argument("\"pop_back\" not supported by this container");
	  }

	  virtual void PopFront(TioData* key, TioData* value, TioData* metadata)
	  {
		  throw std::invalid_argument("\"pop_front\" not supported by this container");
	  }

	  virtual void Set(const TioData& key, const TioData& value, const TioData& metadata)
	  {
		  if(!key)
			  throw std::invalid_argument("invalid key");

		  data_[key.AsSz()] = ValueAndMetadata(value, metadata);

		  dispatcher_.RaiseEvent("set", key, value, metadata);
	  }

	  virtual void Insert(const TioData& key, const TioData& value, const TioData& metadata)
	  {
		  if(!key)
			  throw std::invalid_argument("invalid key");

		  string keyString = key.AsSz();

		  if(key_found(data_, keyString))
			  throw std::invalid_argument("already exits");

		  data_[keyString] = ValueAndMetadata(value, metadata);

		  dispatcher_.RaiseEvent("insert", key, value, metadata);
	  }

	  virtual void Delete(const TioData& key, const TioData& value, const TioData& metadata)
	  {
		  if(!key)
			  throw std::invalid_argument("invalid key");

		  string keyString = key.AsSz();

		  DataMap::iterator i = data_.find(keyString);

		  if(i == data_.end())
			  throw std::invalid_argument("key not found");

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
			  throw std::runtime_error("this container supports only querystr=null");

		  DataMap::const_iterator start, end;

		  if(startOffset == 0 && endOffset == 0)
		  {
			  start = data_.begin();
			  end = data_.end();
		  }
		  else
		  {
			  if(endOffset == 0)
			  {
				  startOffset = NormalizeForQueries(startOffset, data_.size());
				  end = data_.end();
				  start = data_.begin();
				  std::advance(start, startOffset);
			  }
			  else
			  {
				  NormalizeQueryLimits(&startOffset, &endOffset, data_.size());
				  start = end = data_.begin();
				  
				  std::advance(start, startOffset);
				  std::advance(end, endOffset);
			  }
		  }


		  //
		  // We will copy all items to a new container to support multithread.
		  // It can be slow, but it will lock the thread of the client who is
		  // asking for the snapshot, not the whole server.
		  // But yes, we should find a way to make it better
		  //
		  VectorResultSet::ContainerT resultSetItems;

		  resultSetItems.reserve(distance(start, end));

		  for(; start != end; ++start)
			  resultSetItems.push_back(make_tuple(start->first, start->second.value, start->second.metadata));

		  return shared_ptr<ITioResultSet>(
			  new VectorResultSet(std::move(resultSetItems), TIONULL));
	  }

	  virtual unsigned int Subscribe(EventSink sink, const string& start)
	  {
		  //
		  // No start, we will just send the updates. Must send start=0
		  // if client wants current data as well
		  //
		  if(start == "")
		  {
			  sink("snapshot_end", TIONULL, TIONULL, TIONULL);
			  return dispatcher_.Subscribe(sink);
		  }

		  //
		  // we will accept 0 as start index to stay compatible
		  // with vector
		  //
		  DataMap::const_iterator startIterator = data_.begin();

		  //
		  // start == 0 is a very common case, so we can skip all the math
		  //
		  if(!start.empty() && start != "0")
		  {
			  int index = 0;
			  bool isNumeric = false;
			  
			  try
			  {
				  index = lexical_cast<int>(start);
				  index = NormalizeIndex(index, data_.size());
				  isNumeric = true;
			  }
			  catch(std::exception&)
			  {
			  }

			  if(isNumeric)
			  {
				  if(index + 1 > static_cast<int>(data_.size()))
					  throw std::invalid_argument("out of bounds");

				  startIterator = data_.begin();
				  std::advance(startIterator, index);
			  }
			  else
			  {
				  startIterator = data_.find(start);
				  
				  if(startIterator == data_.end())
					  throw std::invalid_argument("key not found");
			  }
		  }

		  for(DataMap::const_iterator i = startIterator ; i != data_.end() ; ++i)
		  {
			  sink("set", i->first.c_str(), i->second.value, i->second.metadata);
		  }

		  sink("snapshot_end", TIONULL, TIONULL, TIONULL);

		  return dispatcher_.Subscribe(sink);
	  }
	  virtual void Unsubscribe(unsigned int cookie)
	  {
		  dispatcher_.Unsubscribe(cookie);
	  }

	  virtual void GetRecord(const TioData& searchKey, TioData* key, TioData* value, TioData* metadata)
	  {
		  pair<string, ValueAndMetadata> p = GetInternalRecord(searchKey);
		  const ValueAndMetadata& data = p.second;

		  //
		  // value can be accessed by their numeric indexes (it's how someone
		  // iterates over all values). In this case, the real keys will be returned
		  //
		  if(key)
			*key = p.first;

		  if(value)
			  *value = data.value;

		  if(metadata)
			  *metadata = data.metadata;
	  }

};

}}
