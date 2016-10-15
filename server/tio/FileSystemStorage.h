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
#include "MemoryStorage.h"

namespace tio {
	namespace FileSystemStorage
	{
		using std::make_tuple;
		using std::make_shared;

		namespace filesystem = boost::filesystem;

		using filesystem::directory_iterator;
		using filesystem::path;
		using filesystem::directory_iterator;
		using filesystem::is_regular_file;
		using filesystem::is_directory;
		using boost::locale::conv::utf_to_utf;

		static const unsigned MAX_FILE_SIZE = 10 * 1024 * 1024;


		bool IsAbsoluteAndNoEvilPath(const char* path)
		{
			//
			// TODO: implement
			//
			return true;
		}

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

			FileDirectoryStorage(filesystem::path& directoryPath, const string& name, const string& type)
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
				if(!value)
					throw std::invalid_argument("value argument can't be null");

				if(!IsAbsoluteAndNoEvilPath(searchKey.AsSz()))
					throw std::invalid_argument("key not found");

				logdb::File f;
				path fullPath = directoryPath_ / searchKey.AsSz();

				bool b = f.Open(fullPath.generic_string().c_str());

				if(!b)
					throw std::invalid_argument("key not found");

				auto fileSize = f.GetFileSize();

				if(fileSize > MAX_FILE_SIZE)
					throw std::invalid_argument("file to big");

				char* buffer = value->AllocStringAndGetBuffer(fileSize);

				auto read = f.Read(buffer, fileSize);

				if(read != fileSize)
					throw std::invalid_argument("i/o error");

				*key = searchKey;
			}

			virtual unsigned int Subscribe(EventSink sink, const string& start)
			{
				throw std::invalid_argument("not supported");
			}

			virtual void Unsubscribe(unsigned int cookie)
			{
				throw std::invalid_argument("not supported");
			}

			virtual string Get(const string& key)
			{
				TioData nada, value;
				GetRecord(key, &nada, &value, nullptr);
				return value.AsString();
			}
			virtual void Set(const string& key, const string& value)
			{
				throw std::invalid_argument("not supported");
			}
		};


		class FileSystemStorageManager : public ITioStorageManager
		{
			path rootPath_;

			const vector<string> SupportedTypes = { "directory" };

			set<string> directoryList_;

			void ReloadDirectoryList()
			{
				directoryList_.clear();
				ReloadDirectoryList(rootPath_);
			}

			void ReloadDirectoryList(const path& currentPath)
			{
				auto pathPrefixSize = rootPath_.generic_string().size() + 1;

				for (auto& entry : directory_iterator(currentPath))
				{
					if (!is_directory(entry.path()))
						continue;

					directoryList_.emplace(entry.path().generic_string().substr(pathPrefixSize));

					ReloadDirectoryList(entry.path());
				}
			}

		public:

			FileSystemStorageManager(const string& rootPath) : rootPath_(rootPath)
			{

			}

			virtual std::vector<string> GetSupportedTypes()
			{
				return SupportedTypes;
			}

			static bool IsValidPath(const string& containerName)
			{
				//
				// TODO: implement
				//
				return true;
			}

			inline bool Exists(const string& containerType, const string& containerName)
			{
				if(containerType == "directory")
					throw std::invalid_argument("invalid container type");

				if(!IsValidPath(containerName))
					throw std::invalid_argument("invalid container name");

				auto nativePath = utf_to_utf<path::value_type>(containerName);

				auto fullPath = rootPath_ / nativePath;

				return is_directory(fullPath);
			}

			void CheckType(const string& type)
			{
				if (std::find(begin(SupportedTypes), end(SupportedTypes), type) == end(SupportedTypes))
					throw std::invalid_argument("storage type not supported");
			}
						
			void DeleteStorage(const string& containerType, const string& containerName)
			{
				throw std::invalid_argument("not supported");
			}

			virtual vector<StorageInfo> GetStorageList()
			{
				vector<StorageInfo> ret;
				
				if (directoryList_.empty())
					ReloadDirectoryList();

				for (auto& v : directoryList_)
					ret.push_back({ v, "directory" });
				
				return ret;
			}

			virtual pair<shared_ptr<ITioStorage>, shared_ptr<ITioPropertyMap> >
				OpenStorage(const string& type, const string& name)
			{
				if(directoryList_.find(name) == cend(directoryList_))
					throw std::invalid_argument("container not found");

				pair<shared_ptr<ITioStorage>, shared_ptr<ITioPropertyMap> > ret;

				ret.first = make_shared<FileDirectoryStorage>(rootPath_ / name, name, "directory");
				ret.second = make_shared<MemoryStorage::MemoryPropertyMap>();
				
				return ret;
			}

			virtual pair<shared_ptr<ITioStorage>, shared_ptr<ITioPropertyMap> >
				CreateStorage(const string& type, const string& name)
			{
				throw std::invalid_argument("not supported");
			}

		};
	}
}
