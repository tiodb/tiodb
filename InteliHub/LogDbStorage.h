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

#include "logdb.h"

namespace tio {
	namespace LogDbStorage
	{
		using std::make_tuple;

		bool TioDataToLdbData(const TioData& tioData, logdb::LdbData* ldbData)
		{
			if(!tioData || !ldbData)
				return false;

			size_t size = tioData.GetSerializedSize();

			tioData.Serialize(ldbData->Alloc(size), size);

			return true;
		}

		bool LdbDataToTioData(const logdb::LdbData* ldbData, TioData* tioData)
		{
			if(!tioData || !ldbData)
				return false;

			tioData->Deserialize(ldbData->GetBuffer(), ldbData->GetSize());

			return true;
		}

		class ConverterHelper
		{
			logdb::LdbData ldbKey, ldbValue, ldbMetadata;
			bool hasKey, hasValue, hasMetadata;

		public:
			ConverterHelper(const TioData& key, const TioData& value, const TioData& metadata)
			{
				FromTioData(key, value, metadata);
				hasKey = !!key; // forces bool convertion. I know, it's ugly...
				hasValue = !!value;
				hasMetadata = !!metadata;
			}

			ConverterHelper()
			{
				hasKey = hasMetadata = hasValue = true;
			}

			logdb::LdbData* GetLdbKey() { return hasKey ? &ldbKey : NULL;}
			logdb::LdbData* GetLdbValue() { return hasValue ? &ldbValue: NULL;}
			logdb::LdbData* GetLdbMetadata() { return hasMetadata ? &ldbMetadata: NULL;}

			void FromTioData(const TioData& key, const TioData& value, const TioData& metadata)
			{
				TioDataToLdbData(key, &ldbKey);
				TioDataToLdbData(value, &ldbValue);
				TioDataToLdbData(metadata, &ldbMetadata);
			}

			void ToTioData(TioData* key, TioData* value, TioData* metadata)
			{
				LdbDataToTioData(&ldbKey, key);
				LdbDataToTioData(&ldbValue, value);
				LdbDataToTioData(&ldbMetadata, metadata);
			}
		};

		class LogDbVectorStorage : 
			boost::noncopyable,
			public std::enable_shared_from_this<LogDbVectorStorage>,
			public ITioStorage,
			public ITioPropertyMap
		{
		public:
			enum AccessType
			{
				RecordNumber,
				Map
			};

		private:
			logdb::Ldb& ldb_;
			logdb::Ldb::TABLE_INFO* tableInfo_;
			string type_, name_;
			EventDispatcher dispatcher_;
			AccessType accessType_;
		public:

			LogDbVectorStorage(logdb::Ldb& ldb, logdb::Ldb::TABLE_INFO* tableInfo,
				const string& name, const string& type, AccessType accessType) 
				: type_(type), name_(name), accessType_(accessType), 
				ldb_(ldb), tableInfo_(tableInfo)
			{

			}

			~LogDbVectorStorage()
			{
				return;
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
				return ldb_.GetRecordCount(tableInfo_);
			}

			virtual void PushBack(const TioData& key, const TioData& value, const TioData& metadata)
			{
				if(accessType_ != RecordNumber)
					throw std::runtime_error("\"push_back\" not supported by this container");

				CheckValue(value);

				ConverterHelper converter(TIONULL, value, metadata);

				DWORD index = ldb_.Append(tableInfo_, NULL, converter.GetLdbValue(), converter.GetLdbMetadata());

				if(index == logdb::LDB_INVALID_RECNO)
					throw std::runtime_error("error appending record");

				dispatcher_.RaiseEvent("push_back", (int)ldb_.GetRecordCount(tableInfo_), value, metadata);
			}

			virtual void PushFront(const TioData& key, const TioData& value, const TioData& metadata)
			{
				if(accessType_ != RecordNumber)
					throw std::runtime_error("not supported");

				CheckValue(value);
				ConverterHelper converter(key, value, metadata);

				ldb_.InsertByIndex(tableInfo_,0, NULL, converter.GetLdbValue(), converter.GetLdbMetadata());
				dispatcher_.RaiseEvent("push_front", 0, value, metadata);
			}

		private:
			void _Pop(size_t recordIndex, TioData* key, TioData* value, TioData* metadata)
			{
				ConverterHelper helper;

				ldb_.GetByIndex(tableInfo_, recordIndex, helper.GetLdbKey(), helper.GetLdbValue(), helper.GetLdbMetadata());

				ldb_.DeleteByIndex(tableInfo_, recordIndex);

				helper.ToTioData(key, value, metadata);
			}
		public:
			virtual void PopBack(TioData* key, TioData* value, TioData* metadata)
			{
				if(accessType_ != RecordNumber)
					throw std::runtime_error("\"pop_back\" not supported by this container");

				size_t recordCount = ldb_.GetRecordCount(tableInfo_);

				if(recordCount == 0)
					throw std::invalid_argument("empty");

				size_t recordIndex = recordCount - 1;

				TioData itemValue, itemMetadata;

				_Pop(recordIndex, NULL, &itemValue, &itemMetadata);

				if(key)
					*key = (int)recordIndex;

				if(value)
					*value = itemValue;
				
				if(metadata)
					*metadata = itemMetadata;

				dispatcher_.RaiseEvent("pop_back", 
					(int)recordIndex, // returned key is the item index
					itemValue,
					itemMetadata);
			}

			virtual void PopFront(TioData* key, TioData* value, TioData* metadata)
			{
				if(accessType_ != RecordNumber)
					throw std::runtime_error("\"pop_front\" not supported by this container");

				if(ldb_.GetRecordCount(tableInfo_) == 0)
					throw std::invalid_argument("empty");

				_Pop(0, key, value, metadata);

				dispatcher_.RaiseEvent("pop_front", 
					0, 
					value ? *value : TIONULL,
					metadata ? *metadata : TIONULL);
			}

			void CheckValue(const TioData& value)
			{
				if(!value)
					throw std::invalid_argument("value??");
			}

			virtual void Set(const TioData& key, const TioData& value, const TioData& metadata)
			{
				if(!key)
					throw std::invalid_argument("key??");

				CheckValue(value);

				if(accessType_ == RecordNumber)
				{
					ConverterHelper converter(TIONULL, value, metadata);

					ldb_.SetByIndex(tableInfo_, key.AsInt(), NULL, converter.GetLdbValue(), converter.GetLdbMetadata());
				}
				else 
				{
					BOOST_ASSERT(accessType_ == Map);

					//
					// keys must be strings. This call will raise an exception
					// if it isn't
					//
					key.AsSz();

					ConverterHelper converter(key, value, metadata);

					ldb_.Set(tableInfo_, 0, *converter.GetLdbKey(), converter.GetLdbValue(), converter.GetLdbMetadata());
				}

				dispatcher_.RaiseEvent("set", key, value, metadata);
			}

			virtual void Insert(const TioData& key, const TioData& value, const TioData& metadata)
			{
				if(!key)
					throw std::invalid_argument("key??");

				CheckValue(value);

				if(accessType_ == RecordNumber)
				{
					size_t recordNumber = key.AsInt();

					ConverterHelper converter(TIONULL, value, metadata);

					DWORD index = ldb_.InsertByIndex(tableInfo_, recordNumber, NULL, converter.GetLdbValue(), converter.GetLdbMetadata());

					if(index == logdb::LDB_INVALID_RECNO)
						throw std::invalid_argument("invalid index");
				}
				else
				{
					BOOST_ASSERT(accessType_ == Map);

					//
					// keys must be strings. This call will raise an exception
					// if it isn't
					//
					key.AsSz();

					ConverterHelper converter(key, value, metadata);

					size_t recordNumber = ldb_.FindKey(tableInfo_, 0, *converter.GetLdbKey());

					if(recordNumber != logdb::LDB_INVALID_RECNO)
						throw std::invalid_argument("already exists");

					ldb_.Append(tableInfo_, converter.GetLdbKey(), converter.GetLdbValue(), converter.GetLdbMetadata());
				}

				dispatcher_.RaiseEvent("insert", key, value, metadata);
			}

			virtual void Delete(const TioData& key, const TioData& value, const TioData& metadata)
			{
				if(!key)
					throw std::invalid_argument("key??");

				if(accessType_ == RecordNumber)
				{
					DWORD dw = ldb_.DeleteByIndex(tableInfo_, key.AsInt());

					if(dw == logdb::LDB_INVALID_RECNO)
						throw std::invalid_argument("invalid index");
				}	
				else
				{
					BOOST_ASSERT(accessType_ == Map);

					ConverterHelper helper(key, TIONULL, TIONULL);

					DWORD recno = ldb_.Delete(tableInfo_, 0, *helper.GetLdbKey());

					if(recno == logdb::LDB_INVALID_RECNO)
						throw std::invalid_argument("invalid index");
				}

				dispatcher_.RaiseEvent("delete", key, TIONULL, TIONULL);
			}

			virtual void Clear()
			{
				ldb_.ClearAllRecords(tableInfo_);
			}

			virtual shared_ptr<ITioResultSet> Query(int startOffset, int endOffset, const TioData& query)
			{
				if(!query.IsNull())
					throw std::runtime_error("query string not supported by this container");

				NormalizeQueryLimits(&startOffset, &endOffset, GetRecordCount());

				VectorResultSet::ContainerT resultSetItems;

				resultSetItems.reserve(endOffset - startOffset);

				for(int index = startOffset; index != endOffset; ++index)
				{
					TioData key, value, metadata;
					GetRecord(TioData(index), &key, &value, &metadata);
					resultSetItems.push_back(make_tuple(key, value, metadata));
				}

				return shared_ptr<ITioResultSet>(
					new VectorResultSet(std::move(resultSetItems), TIONULL));
			}

			virtual void GetRecord(const TioData& searchKey, TioData* key,  TioData* value, TioData* metadata)
			{
				if(!searchKey)
					throw std::invalid_argument("key??");

				ConverterHelper helper;

				if(accessType_ == RecordNumber)
				{
					int index = NormalizeIndex(searchKey.AsInt(), ldb_.GetRecordCount(tableInfo_));

					DWORD dw = ldb_.GetByIndex(tableInfo_, 
						index, 
						helper.GetLdbKey(),
						helper.GetLdbValue(),
						helper.GetLdbMetadata());

					if(dw == logdb::LDB_INVALID_RECNO)
						throw std::invalid_argument("invalid index");

					helper.ToTioData(NULL, value, metadata);

					if(key)
						*key = index;
				}
				else
				{
					BOOST_ASSERT(accessType_ == Map);

					helper.FromTioData(searchKey, TIONULL, TIONULL);

					DWORD recordIndex = logdb::LDB_INVALID_RECNO;

					if(searchKey.GetDataType() == TioData::Int)
					{
						recordIndex = ldb_.GetByIndex(tableInfo_, searchKey.AsInt(), 
							helper.GetLdbKey(), helper.GetLdbValue(), helper.GetLdbMetadata());
					}
					else if(searchKey.GetDataType() == TioData::String)
					{
						recordIndex = ldb_.Get(tableInfo_, 0, *helper.GetLdbKey(), 
							helper.GetLdbValue(), helper.GetLdbMetadata());
					}

					if(recordIndex == logdb::LDB_INVALID_RECNO)
						throw std::invalid_argument("key not found");

					helper.ToTioData(key, value, metadata);
				}

				
			}

			virtual unsigned int Subscribe(EventSink sink, const string& start)
			{
				size_t startIndex = 0;

				if(start.empty())
				{
					sink("snapshot_end", TIONULL, TIONULL, TIONULL);
					return dispatcher_.Subscribe(sink);
				}

				try
				{
					startIndex = lexical_cast<int>(start);
				}
				catch(std::exception&)
				{
					throw std::invalid_argument("invalid start index");
				}

				if(accessType_ == Map && startIndex != 0)
						throw std::invalid_argument("invalid start");

				size_t size = ldb_.GetRecordCount(tableInfo_);

				for(size_t x = startIndex ; x < size ; x++)
				{
					ConverterHelper helper;
					TioData key, value, metadata;

					if(accessType_ == RecordNumber)
					{
						ldb_.GetByIndex(tableInfo_, x, NULL, helper.GetLdbValue(), helper.GetLdbMetadata());
						helper.ToTioData(NULL, &value, &metadata);
						key.Set((int)x);
						sink("push_back", key, value, metadata);
					}
					else
					{
						ldb_.GetByIndex(tableInfo_, x, helper.GetLdbKey(), helper.GetLdbValue(), helper.GetLdbMetadata());
						helper.ToTioData(&key, &value, &metadata);
						sink("set", key, value, metadata);
					}			
				}

				sink("snapshot_end", TIONULL, TIONULL, TIONULL);
				return dispatcher_.Subscribe(sink);
			}
			virtual void Unsubscribe(unsigned int cookie)
			{
				dispatcher_.Unsubscribe(cookie);
			}

			virtual string Get(const string& key)
			{
				BOOST_ASSERT(accessType_ == Map);

				if(key.empty())
					throw std::invalid_argument("invalid key");

				logdb::LdbData keyData(key.c_str(), key.size(), logdb::LdbData::dontCopyBuffer);
				logdb::LdbData valueData;

				DWORD dw = ldb_.Get(tableInfo_, 0, keyData, &valueData, NULL);

				if(dw == logdb::LDB_INVALID_RECNO || valueData.GetSize() == 0)
					throw std::invalid_argument("invalid key");

				string ret((char*)valueData.GetBuffer(), valueData.GetSize());

				return ret;

			}
			virtual void Set(const string& key, const string& value)
			{
				BOOST_ASSERT(accessType_ == Map);

				if(key.empty() || value.empty())
					throw std::invalid_argument("invalid key");

				logdb::LdbData keyData(key.c_str(), key.size(), logdb::LdbData::dontCopyBuffer);
				logdb::LdbData valueData(value.c_str(), value.size(), logdb::LdbData::dontCopyBuffer);

				DWORD dw = ldb_.Set(tableInfo_, 0, keyData, &valueData, NULL);

				if(dw == logdb::LDB_INVALID_RECNO)
					throw std::invalid_argument("internal error");

				return;
			}
		};


		class LogDbStorageManager: public ITioStorageManager
		{
			//
			// weak_ptr so we'll keep storage open just for cache
			//
			typedef std::map<string, pair<weak_ptr<ITioStorage>, weak_ptr<ITioPropertyMap> > > StorageMap;
			StorageMap containers_;
			string path_;
			logdb::Ldb ldb_;

		public:

			LogDbStorageManager(const string& path) : path_(path)
			{
				if(!path_.empty())
				{
					char last = *path_.rbegin();
					if(last != '\\' && last != '/')
						path_ += '/'; // works for win32 and *nix
				}

				path_ += "tio.logdb";

				bool b = ldb_.Create(path_.c_str());

				if(!b)
					throw std::runtime_error("error creating logdb file");
			}

			virtual std::vector<string> GetSupportedTypes()
			{
				std::vector<string> ret;

				ret.push_back("persistent_list");
				ret.push_back("persistent_map");

				return ret;
			}

			inline bool Exists(const string& containerType, const string& containerName)
			{
				const string& fullName = GenerateDataTableName("data", containerType, containerName);
				return key_found(containers_, fullName);
			}

			inline string GenerateNamelessName()
			{
				static unsigned int nameless = 0;
				return string("__nameless") + lexical_cast<string>(nameless) + "__";
			}

			void CheckType(const string& type)
			{
				if(type != "persistent_list" && type != "persistent_map")
					throw std::invalid_argument("storage type not supported");
			}

			string GenerateDataTableName(const string& tableType, const string& containerType, const string& containerName)
			{
				return containerType + "|" + tableType + "|" + containerName;
			}

			void DeleteStorage(const string& containerType, const string& containerName)
			{
				logdb::Ldb::TABLE_INFO* tableInfo;

				//
				// data
				//
				tableInfo = ldb_.OpenTable(GenerateDataTableName("data", containerType, containerName));

				if(!tableInfo)
					throw std::invalid_argument("no such data container");

				bool b = ldb_.DeleteTable(tableInfo);

				if(!b)
					throw std::invalid_argument("error deleting data container");

				//
				// properties
				//
				tableInfo = ldb_.OpenTable(GenerateDataTableName("properties", containerType, containerName));

				if(!tableInfo)
					return; // well, we already deleted data, maybe it doesn't have properties, who knows...

				ldb_.DeleteTable(tableInfo);

				return;
			}

			virtual pair<shared_ptr<ITioStorage>, shared_ptr<ITioPropertyMap> > 
				CreateOrOpenStorage(const string& type, const string& name, bool create)
			{
				typedef pair<shared_ptr<ITioStorage>, shared_ptr<ITioPropertyMap>> ReturnType;
				CheckType(type);

				if(name.empty())
					throw std::invalid_argument("invalid name");

				string dataTableName = GenerateDataTableName("data", type, name);
				string propertiesTableName = GenerateDataTableName("properties", type, name);

				StorageMap::iterator i = containers_.find(dataTableName);

				//
				// no matter if it's open or create, if it already
				// exists, here it goes
				//
				if(i != containers_.end() && !i->second.first.expired() && !i->second.second.expired())
					return ReturnType(i->second.first.lock(), i->second.second.lock());

				LogDbVectorStorage::AccessType accessType;

				if(type == "persistent_list")
					accessType = LogDbVectorStorage::RecordNumber;
				else 
				{	
					BOOST_ASSERT(type == "persistent_map");
					accessType = LogDbVectorStorage::Map;
				}

				logdb::Ldb::TABLE_INFO* dataTableInfo;
				logdb::Ldb::TABLE_INFO* propertiesTableInfo;

				if(create)
				{
					dataTableInfo = ldb_.CreateTable(dataTableName);
					propertiesTableInfo = ldb_.CreateTable(propertiesTableName);
				}
				else
				{
					dataTableInfo = ldb_.OpenTable(dataTableName);

					if(!dataTableInfo)
						throw std::invalid_argument("no such data container");

					propertiesTableInfo = ldb_.OpenTable(propertiesTableName);

				}

				shared_ptr<ITioStorage> container = shared_ptr<ITioStorage>(
					new LogDbVectorStorage(
					ldb_,
					dataTableInfo, 
					name,
					type, 
					accessType));

				shared_ptr<ITioPropertyMap> propertyMap = shared_ptr<ITioPropertyMap>(
					new LogDbVectorStorage(
					ldb_,
					propertiesTableInfo,
					name,
					type,
					LogDbVectorStorage::Map));

				StorageMap::mapped_type& p = (i != containers_.end()) ? i->second : containers_[dataTableName];

				p.first = container;
				p.second = propertyMap;

				return ReturnType(p.first.lock(), p.second.lock());
			}

			virtual vector<StorageInfo> GetStorageList()
			{
				vector<StorageInfo> ret;

				vector<string> names = ldb_.GetTableList();

				for(vector<string>::const_iterator i = names.begin() ; i != names.end() ; ++i)
				{
					StorageInfo si;

					vector<string> parts;
					boost::algorithm::split(parts, *i, boost::algorithm::is_any_of("|"));

					ASSERT(parts.size() == 3);

					//
					// should be [container type]|[table type]|[name]
					// [table type] = data | properties
					//
					if(parts.size() != 3)
						continue;
					
					if(parts[1] != "data")
						continue;
						
					si.type = parts[0];
					si.name = parts[2];

					ret.push_back(si);
				}

				return ret;
			}

			virtual pair<shared_ptr<ITioStorage>, shared_ptr<ITioPropertyMap> > 
				OpenStorage(const string& type, const string& name)
			{
				return CreateOrOpenStorage(type, name, false);
			}

			virtual pair<shared_ptr<ITioStorage>, shared_ptr<ITioPropertyMap> > 
				CreateStorage(const string& type, const string& name)
			{
				return CreateOrOpenStorage(type, name, true);
			}

		};
	}
}
