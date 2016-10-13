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
	namespace FileSystemStorage
	{
		using std::make_tuple;

		namespace filesystem = boost::filesystem;

		using filesystem::directory_iterator;
		using filesystem::path;
		using filesystem::directory_iterator;
		using filesystem::is_regular_file;
		using filesystem::is_directory;

		class FileDirectoryStorage :
			boost::noncopyable,
			public std::enable_shared_from_this<FileDirectoryStorage>,
			public ITioStorage,
			public ITioPropertyMap
		{
			string type_, name_;
			EventDispatcher dispatcher_;
			filesystem::path directoryPath_;
		public:

			FileDirectoryStorage(const string& directoryPath, const string& name, const string& type)
				: directoryPath_(directoryPath)
				, name_(name)
				, type_(type)
			{
				if (!is_directory(directoryPath_))
					throw runtime_error("invalid directory");

			}

			~FileDirectoryStorage()
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
				throw std::invalid_argument("not supported");
			}

			virtual size_t GetRecordCount()
			{
				size_t count = 
					std::count_if(
						directory_iterator(directoryPath_),
						directory_iterator(),
						[](const path& p) -> bool 
						{
							return is_regular_file(p);
						}
					);

				return count;
			}

			virtual void PushBack(const TioData& key, const TioData& value, const TioData& metadata)
			{
				throw std::invalid_argument("not supported");
			}

			virtual void PushFront(const TioData& key, const TioData& value, const TioData& metadata)
			{
				throw std::invalid_argument("not supported");
			}

			virtual void PopBack(TioData* key, TioData* value, TioData* metadata)
			{
				throw std::invalid_argument("not supported");
			}

			virtual void PopFront(TioData* key, TioData* value, TioData* metadata)
			{
				throw std::invalid_argument("not supported");
			}
			
			virtual void Set(const TioData& key, const TioData& value, const TioData& metadata)
			{
				throw std::invalid_argument("not supported");
			}

			virtual void Insert(const TioData& key, const TioData& value, const TioData& metadata)
			{
				throw std::invalid_argument("not supported");
			}

			virtual void Delete(const TioData& key, const TioData& value, const TioData& metadata)
			{
				throw std::invalid_argument("not supported");
			}

			virtual void Clear()
			{
				throw std::invalid_argument("not supported");
			}

			virtual shared_ptr<ITioResultSet> Query(int startOffset, int endOffset, const TioData& query)
			{
				throw std::invalid_argument("not supported");
			}

			virtual void GetRecord(const TioData& searchKey, TioData* key, TioData* value, TioData* metadata)
			{
				throw std::invalid_argument("not supported");
			}

			virtual unsigned int Subscribe(EventSink sink, const string& start)
			{
				throw std::invalid_argument("not supported");
			}

			virtual void Unsubscribe(unsigned int cookie)
			{
				dispatcher_.Unsubscribe(cookie);
			}

			virtual string Get(const string& key)
			{
				throw std::invalid_argument("not supported");

			}
			virtual void Set(const string& key, const string& value)
			{
				throw std::invalid_argument("not supported");
			}
		};


		class FileSystemStorageManager : public ITioStorageManager
		{
			path rootPath_;
		public:

			FileSystemStorageManager(const string& rootPath) : rootPath_(rootPath)
			{

			}

			virtual std::vector<string> GetSupportedTypes()
			{
				return{ "filesystem_map" };
			}

			inline bool Exists(const string& containerType, const string& containerName)
			{
				throw std::invalid_argument("not supported");
			}

			inline string GenerateNamelessName()
			{
				throw std::invalid_argument("not supported");
			}

			void CheckType(const string& type)
			{
				if (type != "persistent_list" && type != "persistent_map")
					throw std::invalid_argument("storage type not supported");
			}
						
			void DeleteStorage(const string& containerType, const string& containerName)
			{
				throw std::invalid_argument("not supported");
			}

			

			virtual vector<StorageInfo> GetStorageList()
			{
				vector<StorageInfo> ret;

				for (const auto& entry : directory_iterator(rootPath_))
				{
					auto x = StorageInfo{ entry.path().c_str(), "filesystem_map" };
					//ret.push_back();
				}

				return ret;
			}

			virtual pair<shared_ptr<ITioStorage>, shared_ptr<ITioPropertyMap> >
				OpenStorage(const string& type, const string& name)
			{
				throw std::invalid_argument("not supported");
			}

			virtual pair<shared_ptr<ITioStorage>, shared_ptr<ITioPropertyMap> >
				CreateStorage(const string& type, const string& name)
			{
				throw std::invalid_argument("not supported");
			}

		};
	}
}
