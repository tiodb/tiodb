#pragma once
#include "Container.h"

namespace tio {
namespace MemoryStorage
{

typedef list<ValueAndMetadata> ListType;

class ListResultSet : public ITioResultSet
{
	TioData source_;
	ListType::const_iterator begin_, current_, end_;
	
	size_t currentIndex_;
public:

	ListResultSet(const TioData& source, 
		ListType::const_iterator begin, 
		ListType::const_iterator end,
		unsigned int beginIndex)
		: source_(source), current_(begin), begin_(begin), end_(end), currentIndex_(beginIndex)
	{
	}

	virtual bool GetRecord(TioData* key, TioData* value, TioData* metadata)
	{
		if(current_ == end_)
			return false;

		if(key)
			key->Set((int)currentIndex_);

		if(value)
			*value = current_->value;

		if(metadata)
			*metadata = current_->metadata;

		return true;
	}

	virtual bool MoveNext()
	{
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
		if(current_ == begin_)
			return false;

		--current_;
		--currentIndex_;

		return true;
	}

	virtual bool AtBegin()
	{
		return current_ == begin_;
	}

	virtual bool AtEnd()
	{
		return current_ == end_;
	}

	virtual TioData Source()
	{
		return source_;
	}

	virtual unsigned int RecordCount()
	{
		return 0xFFFFFFFF;
	}
};

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
		  throw std::invalid_argument("not supported");
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

	ListType::iterator GetOffset(const TioData& key)
	{
		int index = NormalizeIndex(key.AsInt(), data_.size());
		ListType::iterator i;

		//
		// advance a list iterator is expensive. If it's near the end, will
		// walk backwards
		//
		if(index < data_.size() / 2)
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
		ListType::iterator i = GetOffset(key);

		data_.insert(i, ValueAndMetadata(value, metadata));

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

	virtual shared_ptr<ITioResultSet> Query(const TioData& query)
	{
		if(query.IsNull() || (query.GetDataType() == TioData::Sz && *query.AsSz() == '\0'))
			return shared_ptr<ITioResultSet>(new ListResultSet(TIONULL, data_.begin(), data_.end(), 0));

		//
		// int = start index
		//
		if(query.GetDataType() == TioData::Int)
			return shared_ptr<ITioResultSet>(
				new ListResultSet(TIONULL, GetOffset(query.AsInt()), data_.end(), query.AsInt()));
		
		throw std::runtime_error("not supported");		
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
