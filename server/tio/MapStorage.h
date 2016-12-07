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
			EventSink sink_;

			typedef tio_fast_lock mutex_t;
			typedef lock_guard<mutex_t> lock_guard_t;

			mutex_t mutex_;

			void Publish(ContainerEvent eventId, const TioData& k, const TioData& v, const TioData& m)
			{
				if (!sink_)
					return;

				sink_(GetId(), eventId, k, v, m);
			}

			inline pair<const string, ValueAndMetadata> GetInternalRecord(const TioData& key)
			{
				if (key.GetDataType() == TioData::Int)
				{
					int offset = NormalizeIndex(key.AsInt(), data_.size());

					DataMap::iterator i = data_.begin();
					std::advance(i, offset);

					return *i;
				}

				DataMap::iterator i = data_.find(key.AsSz());

				if (i == data_.end())
					throw std::invalid_argument("key not found");

				return *i;
			}


		public:

			MapStorage(const string& name, const string& type) :
				name_(name),
				type_(type)
			{}

			virtual uint64_t GetId()
			{
				return reinterpret_cast<uint64_t>(this);
			}

			//
			// ITioPropertyMap
			//
			virtual void Set(const string& key, const string& value)
			{
				throw std::runtime_error("can't change special property");
			}

			virtual string Get(const string& key)
			{
				if (key != "__keys__")
					throw std::invalid_argument("key not found");

				{
					lock_guard_t lock(mutex_);

					if (data_.empty())
						return string();

					stringstream buffer;

					for (DataMap::const_iterator i = data_.begin(); i != data_.end(); ++i)
					{
						buffer << i->first << "\r\n";
					}

					//
					// delete last \r\n
					//
					string str = buffer.str();

					return string(str.begin(), str.end() - 2);
				}

				
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
				lock_guard_t lock(mutex_);
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
				if (!key)
					throw std::invalid_argument("invalid key");

				{
					lock_guard_t lock(mutex_);

					data_[key.AsSz()] = ValueAndMetadata(value, metadata);

					Publish(EVENT_SET, key, value, metadata);
				}
			}

			virtual void Insert(const TioData& key, const TioData& value, const TioData& metadata)
			{
				if (!key)
					throw std::invalid_argument("invalid key");

				string keyString = key.AsSz();

				{
					lock_guard_t lock(mutex_);

					if (key_found(data_, keyString))
						throw std::invalid_argument("already exits");

					data_[keyString] = ValueAndMetadata(value, metadata);

					Publish(EVENT_SET, key, value, metadata);
				}
			}

			virtual void Delete(const TioData& key, const TioData& value, const TioData& metadata)
			{
				if (!key)
					throw std::invalid_argument("invalid key");

				string keyString = key.AsSz();

				{
					lock_guard_t lock(mutex_);

					DataMap::iterator i = data_.find(keyString);

					if (i == data_.end())
						throw std::invalid_argument("key not found");

					data_.erase(i);

					Publish(EVENT_DELETE, key, value, metadata);
				}
			}

			virtual void Clear()
			{
				{
					lock_guard_t lock(mutex_);
					data_.clear();

					Publish(EVENT_CLEAR, TIONULL, TIONULL, TIONULL);
				}
			}

			virtual shared_ptr<ITioResultSet> Query(int startOffset, int endOffset, const TioData& query)
			{
				if (!query.IsNull())
					throw std::runtime_error("this container supports only querystr=null");

				{
					lock_guard_t lock(mutex_);

					DataMap::const_iterator start, end;

					if (startOffset == 0 && endOffset == 0)
					{
						start = data_.begin();
						end = data_.end();
					}
					else
					{
						if (endOffset == 0)
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

					for (; start != end; ++start)
						resultSetItems.push_back(make_tuple(start->first, start->second.value, start->second.metadata));

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
				pair<string, ValueAndMetadata> p;

				{
					lock_guard_t lock(mutex_);
					p = GetInternalRecord(searchKey);
				}

				//
				// value can be accessed by their numeric indexes (it's how someone
				// iterates over all values). In this case, the real keys will be returned
				//
				if (key)
					*key = p.first;

				if (value)
					*value = p.second.value;

				if (metadata)
					*metadata = p.second.metadata;
			}

		};

	}
}
