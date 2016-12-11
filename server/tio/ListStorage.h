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
		using std::make_tuple;

		typedef list<ValueAndMetadata> ListType;

		class ListStorage :
			boost::noncopyable,
			public ITioStorage
		{
		private:
			ListType data_;
			const string name_, type_;
			EventSink sink_;

			uint64_t revNum_;

			typedef tio_fast_lock mutex_t;
			typedef lock_guard<mutex_t> lock_guard_t;

			mutex_t mutex_;

			void UpdateRevNumAndPublish(ContainerEventCode eventCode, const TioData& k, const TioData& v, const TioData& m)
			{
				++revNum_;

				if (!sink_)
					return;

				sink_(GetId(), eventCode, k, v, m);
			}

		public:

			ListStorage(const string& name, const string& type) :
				name_(name),
				type_(type),
				revNum_(0)
			{}

			virtual uint64_t GetId()
			{
				return reinterpret_cast<uint64_t>(this);
			}

			virtual uint64_t GetRevNum()
			{
				lock_guard_t lock(mutex_);
				return revNum_;
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
				throw std::invalid_argument("\"command\" not supported by the container");
			}

			virtual size_t GetRecordCount()
			{
				lock_guard_t lock(mutex_);
				return data_.size();
			}

			virtual void PushBack(const TioData& key, const TioData& value, const TioData& metadata)
			{
				CheckValue(value);

				TioData publishKey;

				{
					lock_guard_t lock(mutex_);
					data_.push_back(ValueAndMetadata(value, metadata));
					
					publishKey = static_cast<int>(data_.size() - 1);
					UpdateRevNumAndPublish(EVENT_CODE_PUSH_BACK, publishKey, value, metadata);
				}
			}

			virtual void PushFront(const TioData& key, const TioData& value, const TioData& metadata)
			{
				CheckValue(value);

				{
					lock_guard_t lock(mutex_);

					data_.push_front(ValueAndMetadata(value, metadata));

					UpdateRevNumAndPublish(EVENT_CODE_PUSH_FRONT, 0, value, metadata);
				}
			}

			virtual void PopBack(TioData* key, TioData* value, TioData* metadata)
			{
				ValueAndMetadata data;
				int index;

				{
					lock_guard_t lock(mutex_);

					if (data_.empty())
						throw std::invalid_argument("empty");

					data = data_.back();
					data_.pop_back();
					index = static_cast<int>(data_.size());

					UpdateRevNumAndPublish(EVENT_CODE_DELETE,
						index,
						value ? *value : TIONULL,
						metadata ? *metadata : TIONULL);
				}

				if (key)
					*key = index;

				if (value)
					*value = data.value;

				if (metadata)
					*metadata = data.metadata;
			}

			virtual void PopFront(TioData* key, TioData* value, TioData* metadata)
			{
				ValueAndMetadata data;
				int index = 0;

				{
					lock_guard_t lock(mutex_);

					if (data_.empty())
						throw std::invalid_argument("empty");

					data = data_.front();
					data_.pop_front();

					UpdateRevNumAndPublish(EVENT_CODE_DELETE,
						index,
						value ? *value : TIONULL,
						metadata ? *metadata : TIONULL);
				}

				if (key)
					*key = index;

				if (value)
					*value = data.value;

				if (metadata)
					*metadata = data.metadata;
			}

			void CheckValue(const TioData& value)
			{
				if (!value)
					throw std::invalid_argument("value??");
			}

			ListType::iterator GetOffset(const TioData& key, size_t* realIndex = NULL, bool canBeTheEnd = false)
			{
				int index = NormalizeIndex(key.AsInt(), data_.size());

				if (realIndex)
					*realIndex = index;

				ListType::iterator i;

				//
				// advance a list iterator is expensive. If it's near the end, will
				// walk backwards
				//
				if (data_.empty())
				{
					i = data_.end();
				}
				else if (index <= static_cast<int>(data_.size() / 2))
				{
					i = data_.begin();
					for (int x = 0; x < index; ++x, ++i)
						;
				}
				else
				{
					i = data_.end();

					int walk = data_.size() - index;

					for (int x = 0; x < walk; ++x, --i)
						;
				}

				return i;
			}

			virtual void Set(const TioData& key, const TioData& value, const TioData& metadata)
			{
				ValueAndMetadata valueAndMetadata;

				{
					lock_guard_t lock(mutex_);
					valueAndMetadata = *GetOffset(key);
					UpdateRevNumAndPublish(EVENT_CODE_SET, key, value, metadata);
				}

				if (value)
					valueAndMetadata.value = value;

				if (metadata)
					valueAndMetadata.metadata = metadata;
			}

			virtual void Insert(const TioData& key, const TioData& value, const TioData& metadata)
			{
				size_t index = key.AsInt();

				ValueAndMetadata valueAndMetadata(value, metadata);

				{
					lock_guard_t lock(mutex_);

					if (index == 0)
						data_.push_front(valueAndMetadata);
					else if (index == data_.size())
						data_.push_back(valueAndMetadata);
					else
						data_.insert(GetOffset(key), valueAndMetadata);

					UpdateRevNumAndPublish(EVENT_CODE_INSERT, key, value, metadata);
				}
			}

			virtual void Delete(const TioData& key, const TioData& value, const TioData& metadata)
			{
				TioData realKey;
				size_t realIndex;

				{
					lock_guard_t lock(mutex_);

					ListType::iterator i = GetOffset(key, &realIndex);

					if (i == data_.end())
						throw std::runtime_error("invalid index");
					
					realKey.Set(static_cast<int>(realIndex));

					data_.erase(i);

					UpdateRevNumAndPublish(EVENT_CODE_DELETE, realKey, value, metadata);
				}
			}

			virtual void Clear()
			{
				lock_guard_t lock(mutex_);

				data_.clear();

				UpdateRevNumAndPublish(EVENT_CODE_CLEAR, TIONULL, TIONULL, TIONULL);
			}

			virtual shared_ptr<ITioResultSet> Query(int startOffset, int endOffset, const TioData& query)
			{
				if (!query.IsNull())
					throw std::runtime_error("query type not supported by this container");

				{
					lock_guard_t lock(mutex_);

					ListType::const_iterator begin, end;

					//
					// if client is asking for a negative index that's bigger than the container,
					// will start from beginning. Ex: if container size is 3 and start = -5, will start from 0
					//
					if (data_.size() == 0)
					{
						begin = end = data_.end();
						startOffset = 0;
					}
					else
					{
						int recordCount = data_.size();

						NormalizeQueryLimits(&startOffset, &endOffset, recordCount);

						if (startOffset == 0)
							begin = data_.begin();
						else if (startOffset == recordCount)
							begin = data_.end();
						else
							begin = GetOffset(startOffset);

						if (endOffset == 0)
							end = data_.begin();
						else if (endOffset == recordCount)
							end = data_.end();
						else
							end = GetOffset(endOffset);
					}

					VectorResultSet::ContainerT resultSetItems;

					resultSetItems.reserve(endOffset - startOffset);

					for (int key = startOffset; begin != end; ++begin, ++key)
						resultSetItems.push_back(make_tuple(TioData(key), begin->value, begin->metadata));

					return shared_ptr<ITioResultSet>(
						new VectorResultSet(std::move(resultSetItems), TIONULL));
				}
			}

			virtual void SetSubscriber(EventSink sink)
			{
				sink_ = sink;
			}

			virtual void RemoveSubscriber()
			{
				sink_ = nullptr;
			}


			virtual void GetRecord(const TioData& searchKey, TioData* key, TioData* value, TioData* metadata)
			{
				lock_guard_t lock(mutex_);

				size_t realIndex = 0;
				ListType::iterator i = GetOffset(searchKey, &realIndex);

				if (key)
					*key = static_cast<int>(realIndex);

				if (value)
					*value = i->value;

				if (metadata)
					*metadata = i->metadata;

			}
		};

	}
}
