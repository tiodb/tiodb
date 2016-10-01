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
#include "MemoryStorage.h"

namespace tio { namespace BdbStorage
{
	using tio::MemoryStorage::MemoryPropertyMap;

	class DbtEx : 
		public Dbt,
		public boost::noncopyable
	{
	private:
		//
		// to avoid allocating from heap if
		// the buffer is small
		//
		unsigned char buffer_[16];

		void Init(const TioData& value, const TioData& metadata)
		{
			size_t size;
			unsigned char* data;

			size = 4; // first DWORD = block count

			if(value)
				size += value.GetSerializedSize();

			if(metadata)
				size += metadata.GetSerializedSize();

			if(size <= sizeof(buffer_))
				data = buffer_;
			else
				data = new unsigned char[size];

			unsigned int* blockCount = (unsigned int*) data;
			*blockCount = 0;
			size -= 4;

			size_t dataSize = 0;

			if(value)
			{
				dataSize = value.Serialize(data + 4, size);
				++*blockCount;
			}

			if(metadata)
			{
				dataSize += metadata.Serialize(
				((unsigned char*)data) + dataSize + 4, 
				size - dataSize);
				++*blockCount;
			}

			BOOST_ASSERT(dataSize == size);
			size += 4;

			set_data(data);
			set_size(size);
		}
	public:
		explicit DbtEx(u_int32_t uint)
		{
			Set(uint);
		}

		DbtEx()
		{
			memset(this, 0, sizeof(DbtEx));
		}

		explicit DbtEx(const char* sz)
		{
			Set(sz);
		}

		DbtEx(const TioData& data, const TioData& metadata)
		{
			Init(data, metadata);
		}

		void Set(const char* sz)
		{
			Free();
			size_t size = strlen(sz) + 1;
			char* data = new char[size];
			memcpy(data, sz, size);
			
			set_data(data);
			set_size(size);
		}
		void Set(u_int32_t uint)
		{
			Free();
			set_size(sizeof(u_int32_t));
			unsigned char* data = buffer_;
			*((u_int32_t*)data) = uint;
			set_data(data);
		}

		void Free()
		{
			unsigned char* data = (unsigned char*) get_data();
			
			if(data && data != buffer_)
				delete data;

			set_data(NULL);
			set_size(0);
		}

		~DbtEx()
		{
			Free();
		}
	};

	void DeserializeBdt(Dbt* bdt,  TioData* value, TioData* metadata)
	{
		if(bdt->get_size() < sizeof(unsigned int))
			throw std::invalid_argument("empty record");

		unsigned int* buffer = (unsigned int*)bdt->get_data();
		unsigned int blockCount, blockSize;
		size_t size = 0;

		blockCount = buffer[0];
		unsigned char* dataBuffer = reinterpret_cast<unsigned char*>(&buffer[1]);

		if(blockCount > 0 && value != NULL)
		{
			size = value->Deserialize(dataBuffer, bdt->get_size());
		}

		if(blockCount < 2 || metadata == NULL)
			return;

		dataBuffer += size;
		blockSize = *((unsigned int*)dataBuffer);
		dataBuffer += sizeof(unsigned int);

		size = metadata->Deserialize(dataBuffer, bdt->get_size() - size);
	}

	using tio::TioData;
	class BdbStorage : 
		boost::noncopyable,
		public ITioStorage,
		public ITioPropertyMap
	{
	public:
		struct BDB_STORAGE_CONFIG
		{
			string name;
			string type; 
			
			string fileName;
			string subname;
			
			DbEnv* env;

			DBTYPE bdb_type;
			u_int32_t flags;

			std::function<DbTxn* ()> GetTransaction;
			std::function<void ()> OnUpdate;
			std::function<void ()> ForceCommit;
		};

	private:

		EventDispatcher dispatcher_;
		BDB_STORAGE_CONFIG config_;
		Db db_;
		DbTxn* transaction_;

		DbTxn* GetTransaction()
		{
			return config_.GetTransaction();
		}

		void GetInternalRecord(const TioData& key, TioData* value, TioData* metadata)
		{
			DbtEx dbtKey;
			Dbt dbtValue;
			GetRecordKey(key, &dbtKey);
			
			try 
			{
				int ret = db_.get(GetTransaction(), &dbtKey, &dbtValue, 0);

				if(ret != 0)
					if(ret == DB_NOTFOUND)
						throw std::invalid_argument("no record for this key");
					else
						throw std::invalid_argument("internal error " + lexical_cast<string>(ret));
			}
			catch (DbException& ex)
			{
				throw std::invalid_argument(ex.what());
			}
			
			DeserializeBdt(&dbtValue, value, metadata);
		}

		void GetInternalRecord(u_int32_t pos, TioData* value, TioData* metadata)
		{
			DbtEx dbtKey(pos);
			Dbt dbtValue;

			try
			{
				db_.get(GetTransaction(), &dbtKey, &dbtValue, 0);
			}
			catch (DbException& ex)
			{
				throw std::invalid_argument(ex.what());
			}

			DeserializeBdt(&dbtValue, value, metadata);
		}

		inline void GetRecordKey(const TioData& key, DbtEx* dbt)
		{
			if(!key)
				throw std::invalid_argument("invalid key");

			if(config_.bdb_type == DB_RECNO && key.GetDataType() == TioData::Int)
			{
				int index = key.AsInt();
				//
				// python like index (-1 for last, -2 for before last, so on)
				//
				if(index < 0)
				{
					size_t recordCount = GetRecordCount();
					if(static_cast<size_t>(-index) > recordCount)
						throw std::invalid_argument("invalid subscript");
					index = recordCount + index;
				}

				try
				{
					// bdb is 1 based
					dbt->Set(static_cast<u_int32_t>(index + 1));
				}
				catch (DbException& ex)
				{
					throw std::invalid_argument(ex.what());
				}

				return;
			}
			else if((config_.bdb_type == DB_HASH || config_.bdb_type == DB_BTREE) && key.GetDataType() == TioData::Sz)
			{
				dbt->Set(key.AsSz());
				return;
			}

			throw std::invalid_argument("invalid key data type");
		}

		void Commit()
		{
			config_.ForceCommit();
		}

		void OnUpdate()
		{
			config_.OnUpdate();
		}

	public:

		BdbStorage(const BDB_STORAGE_CONFIG& config) : 
			config_(config),
			db_(config.env,0)
		{
			try
			{
				if(config_.bdb_type == DB_RECNO)
					db_.set_flags(DB_RENUMBER);

				db_.open(GetTransaction(), 
					config_.fileName.c_str(), 
					config_.subname.c_str(), 
					config_.bdb_type, 
					config_.flags,
					0);

				Commit();
			
			}
			catch(DbException& ex)
			{
				throw std::invalid_argument(ex.what());
			}
		}

		~BdbStorage()
		{
			return;
		}

		virtual string GetName()
		{
			return config_.subname;
		}

		virtual string GetType()
		{
			return config_.type;
		}

		virtual string Command(const string& command)
		{
			throw std::invalid_argument("command not supported");
		}

		virtual size_t GetRecordCount()
		{
			DB_BTREE_STAT* stat = NULL;
			db_.stat(GetTransaction(), &stat, 0);
			
			u_int32_t size = stat->bt_nkeys;
			free(stat); // it's up to us to free it, see bdb docs

			return size;
		}

		virtual void PushBack(const TioData& key, const TioData& value, const TioData& metadata)
		{
			CheckValue(value);

			if(config_.bdb_type != DB_RECNO)
				throw std::runtime_error("invalid operation");

			try
			{
				db_.put(GetTransaction(), NULL, &DbtEx(value, metadata), DB_APPEND);
			}
			catch (DbException& ex)
			{
				throw std::invalid_argument(ex.what());
			}

			OnUpdate();

			dispatcher_.RaiseEvent("push_back", key, value, metadata);
		}

		virtual void PushFront(const TioData& key, const TioData& value, const TioData& metadata)
		{
			CheckValue(value);

			if(config_.bdb_type != DB_RECNO)
				throw std::runtime_error("invalid operation");

			try
			{
				if(GetRecordCount() == 0)
				{
					DbtEx dbtKey;
					GetRecordKey(key, &dbtKey);
					db_.put(GetTransaction(), &dbtKey, &DbtEx(value, metadata), DB_APPEND);
				}
				else
					db_.put(GetTransaction(), &DbtEx(1), &DbtEx(value, metadata), 0);
			}
			catch (DbException& ex)
			{
				throw std::invalid_argument(ex.what());
			}

			OnUpdate();

			dispatcher_.RaiseEvent("push_front", key, value, metadata);
		}

	private:
		void _Pop(u_int32_t pos, TioData* value, TioData* metadata)
		{
			GetInternalRecord(pos, value, metadata);
			try
			{
				db_.del(GetTransaction(), &DbtEx(pos), 0);
			}
			catch (DbException& ex)
			{
				throw std::invalid_argument(ex.what());
			}
			OnUpdate();
		}
	public:

		virtual void PopBack(TioData* key, TioData* value, TioData* metadata)
		{
			if(config_.bdb_type != DB_RECNO)
				throw std::runtime_error("invalid operation");

			size_t recordCount = GetRecordCount();

			if(recordCount == 0)
				throw std::invalid_argument("empty");

			_Pop(recordCount - 1, value, metadata);

			dispatcher_.RaiseEvent("pop_back", 
				key ? *key : TIONULL, 
				value ? *value : TIONULL,
				metadata ? *metadata : TIONULL);
		}

		virtual void PopFront(TioData* key, TioData* value, TioData* metadata)
		{
			if(config_.bdb_type != DB_RECNO)
				throw std::runtime_error("invalid operation");

			size_t recordCount = GetRecordCount();

			if(recordCount == 0)
				throw std::invalid_argument("empty");

			_Pop(0, value, metadata);

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

		virtual void Set(const TioData& key, const TioData& value, const TioData& metadata)
		{
			CheckValue(value);

			DbtEx dbtKey;
			GetRecordKey(key, &dbtKey);

			try
			{
				db_.put(GetTransaction(), &dbtKey, &DbtEx(value, metadata), 0);
			}
			catch (DbException& ex)
			{
				throw std::invalid_argument(ex.what());
			}

			OnUpdate();

			dispatcher_.RaiseEvent("set", key, value, metadata);
		}

		virtual void Insert(const TioData& key, const TioData& value, const TioData& metadata)
		{
			CheckValue(value);

			DbtEx dbtKey;
			GetRecordKey(key, &dbtKey);

			try
			{
				db_.put(GetTransaction(), &dbtKey, &DbtEx(value, metadata), 0);
			}
			catch (DbException& ex)
			{
				throw std::invalid_argument(ex.what());
			}

			OnUpdate();

			dispatcher_.RaiseEvent("insert", key, value, metadata);
		}

		virtual void Delete(const TioData& key, const TioData& value, const TioData& metadata)
		{
			DbtEx dbtKey;
			GetRecordKey(key, &dbtKey);

			try
			{
				db_.del(GetTransaction(), &dbtKey, 0);
			}
			catch (DbException& ex)
			{
				throw std::invalid_argument(ex.what());
			}

			OnUpdate();

			dispatcher_.RaiseEvent("delete", key, value, metadata);
		}

		virtual void Clear()
		{
			u_int32_t count = 0;

			try
			{
				db_.truncate(GetTransaction(), &count, 0);
				db_.compact(GetTransaction(), NULL, NULL, NULL, DB_FREE_SPACE, 0);
			}
			catch (DbException& ex)
			{
				throw std::invalid_argument(ex.what());
			}

			Commit();

			dispatcher_.RaiseEvent("push_front", TIONULL, TIONULL, TIONULL);
		}

		virtual void GetRecord(const TioData& searchKey, TioData* key, TioData* value, TioData* metadata)
		{
			GetInternalRecord(searchKey, value, metadata);
		}

		virtual unsigned int Subscribe(EventSink sink, const string& start)
		{	
			if(config_.bdb_type == DB_HASH || config_.bdb_type == DB_BTREE)
			{

				if(start == "__none__")
					return dispatcher_.Subscribe(sink);
				
				//
				// we will accept 0 as start index to stay compatible
				// with vector
				//
				if(!start.empty() && start != "0")
					throw std::invalid_argument("invalid start");


				Dbc* cursor;
				Dbt keyDbt, dataDbt;

				db_.cursor(GetTransaction(), &cursor, 0);

				for(int ret = cursor->get(&keyDbt, &dataDbt, DB_FIRST) ;
					ret != DB_NOTFOUND ;
					ret = cursor->get(&keyDbt, &dataDbt, DB_NEXT))
				{
					TioData key, value, metadata;

					key.Set((const char*)keyDbt.get_data());

					DeserializeBdt(&dataDbt, &value, &metadata);

					sink("set", key, value, metadata);
				}

			}
			else if(config_.bdb_type == DB_RECNO)
			{
				if(start.empty())
					return dispatcher_.Subscribe(sink);

				try
				{
					Dbc* cursor;
					DbtEx startKey(lexical_cast<u_int32_t>(start) + 1); // bdb is 1 based
					Dbt keyDbt, dataDbt;
				
					db_.cursor(GetTransaction(), &cursor, 0);
					
					keyDbt.set_data(startKey.get_data());
					keyDbt.set_size(startKey.get_size());

					for(int ret = cursor->get(&startKey, &dataDbt, DB_SET) ;
						ret != DB_NOTFOUND ;
						ret = cursor->get(&keyDbt, &dataDbt, DB_NEXT))
					{
						TioData key, value, metadata;

						key.Set((const char*)keyDbt.get_data());

						DeserializeBdt(&dataDbt, &value, &metadata);
						
						sink("push_back", key, value, metadata);
					}
				}
				catch (DbException& ex)
				{
					throw std::invalid_argument(ex.what());
				}
			}

			return dispatcher_.Subscribe(sink);
		}
		virtual void Unsubscribe(unsigned int cookie)
		{
			dispatcher_.Unsubscribe(cookie);
		}


		virtual string Get(const string& key)
		{
			Dbt data;
			
			try
			{
				if(db_.get(GetTransaction(), &DbtEx(key.c_str()), &data, 0) != 0)
					throw std::invalid_argument("invalid key");

			}
			catch(DbException& ex)
			{
				throw std::invalid_argument(ex.what());
			}

			return string((char*)data.get_data(), data.get_size());

		}
		virtual void Set(const string& key, const string& value)
		{
			try
			{
				db_.put(GetTransaction(), &DbtEx(key.c_str()), &DbtEx(value.c_str()), 0);
			}
			catch(DbException& ex)
			{
				throw std::invalid_argument(ex.what());
			}

			OnUpdate();
		}
	};


	class BdbStorageManager: public ITioStorageManager
	{
		string path_;
		DbEnv env_;
		DbTxn* transaction_;

		unsigned int updateCount_;
		unsigned int commitTrigger_;


	public:

		BdbStorageManager(const string& path) : 
			path_(path),
			env_(0)
		{
			env_.set_alloc(&malloc, &realloc, &free);
			env_.set_lg_regionmax(4 * 1024 * 1024);
			env_.set_lk_max_lockers(100 * 1000);
			env_.set_lk_max_objects(100 * 1000);
			env_.set_lk_max_locks(100 * 1000);
			env_.open(path_.c_str(), DB_CREATE | DB_INIT_LOCK | DB_INIT_TXN | DB_INIT_LOG | DB_INIT_MPOOL | DB_RECOVER, 0);
			//env_.txn_begin(NULL, &transaction_, 0);

			//env_.set_flags(DB_TXN_WRITE_NOSYNC, 1);
		}

		DbTxn* GetTransaction()
		{
			return NULL;//return transaction_;
		}

		void ForceCommit()
		{
			return;
			transaction_->commit(0);
			int result = env_.txn_begin(NULL, &transaction_, 0);
			BOOST_ASSERT(result == 0);
		}
		
		void OnUpdate()
		{
			return;
			ForceCommit();
		}

		virtual std::vector<string> GetSupportedTypes()
		{
			std::vector<string> ret;

			ret.push_back("bdb_map");
			ret.push_back("bdb_vector");

			return ret;
		}

		inline bool Exists(const string& containerType, const string& containerName)
		{
			return true;
		}

		virtual void DeleteStorage(const string& type, const string& name)
		{
			throw std::runtime_error("not implemented");
		}

		enum Op {create, open};

		virtual pair<shared_ptr<ITioStorage>, shared_ptr<ITioPropertyMap> > 
			CreateOrOpenStorage(const string& type, const string& name, Op op)
		{
			DBTYPE bdbType;
			
			if(type == "bdb_map")
				bdbType = DB_BTREE;
			else if(type == "bdb_vector")
				bdbType = DB_RECNO;
			else
				throw std::invalid_argument("invalid data container type");

			pair<shared_ptr<ITioStorage>, shared_ptr<ITioPropertyMap> > p;

			BdbStorage::BDB_STORAGE_CONFIG config;

			config.bdb_type = bdbType;
			config.flags = DB_AUTO_COMMIT | (op == create ? DB_CREATE : 0);
			
			config.env = &env_;
			config.type = type;

			config.fileName = "tio.data";
			config.subname = name;
			
			config.ForceCommit = boost::bind(&BdbStorageManager::ForceCommit, this);
			config.OnUpdate = boost::bind(&BdbStorageManager::OnUpdate, this);
			config.GetTransaction = boost::bind(&BdbStorageManager::GetTransaction, this);
			
			tio::BdbStorage::BdbStorage* storage = new tio::BdbStorage::BdbStorage(config);

			config.bdb_type = DB_BTREE;
			config.subname += "__properties";
					
			tio::BdbStorage::BdbStorage* propertyMap = new tio::BdbStorage::BdbStorage(config);

			p.first = shared_ptr<ITioStorage>(storage);
			p.second = shared_ptr<ITioPropertyMap>(propertyMap);

			return p;
		}

		virtual pair<shared_ptr<ITioStorage>, shared_ptr<ITioPropertyMap> > 
			CreateStorage(const string& type, const string& name)
		{
			return CreateOrOpenStorage(type, name, create);
		}

		virtual pair<shared_ptr<ITioStorage>, shared_ptr<ITioPropertyMap> > 
			OpenStorage(const string& type, const string& name)
		{
			return CreateOrOpenStorage(type, name, open);
		}
	};
}}
