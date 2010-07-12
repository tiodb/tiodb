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

#include <map>
#include <list>
#include <vector>
#include <string>
#include <iostream>

#include <sstream>

namespace logdb
{
	
#ifdef _WIN32
	class File
	{
		HANDLE h;
		DWORD _flags;

		bool HasError()
		{
			DWORD err = GetLastError();
			return err != ERROR_SUCCESS && err != ERROR_ALREADY_EXISTS;
		}

	public:
		File() 
			: h(INVALID_HANDLE_VALUE), _flags(FILE_FLAG_NO_BUFFERING)
		{}

		bool Create(const char* name)
		{
			h = CreateFile(name, GENERIC_READ | GENERIC_WRITE, FILE_SHARE_READ, NULL, 
				OPEN_ALWAYS, _flags, NULL);

			return !HasError();
		}

		bool Open(const char* name)
		{
			h = CreateFile(name, GENERIC_READ | GENERIC_WRITE, FILE_SHARE_READ, NULL, 
				OPEN_EXISTING, _flags, NULL);

			return !HasError();
		}

		bool IsValid()
		{
			return h != INVALID_HANDLE_VALUE;
		}

		void Close()
		{
			CloseHandle(h);
			h = INVALID_HANDLE_VALUE;
		}

		DWORD Read(void* buffer, DWORD size)
		{
			ASSERT(IsValid());
			DWORD read = 0;
			ReadFile(h, buffer, size, &read, NULL);
			return read;
		}

		DWORD Write(void* buffer, DWORD size)
		{
			ASSERT(IsValid());
			DWORD written;
			WriteFile(h, buffer, size, &written, NULL);
			return written;
		}

		void SetPointer(DWORD offset)
		{
			ASSERT(IsValid());
			SetFilePointer(h, offset, 0, FILE_BEGIN);
		}

		DWORD GetFileSize()
		{
			ASSERT(IsValid());
			return ::GetFileSize(h, NULL);
		}

		void FlushMetadata()
		{
			ASSERT(IsValid());
			FlushFileBuffers(h);
		}
	};

#else
	#define DWORD unsigned int
	
	class File
	{
		int _file;	

	public:
		File() 
			: _file(-1)
		{}

		bool Create(const char* name)
		{
			_file = open(name, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
			return _file != -1;
		}

		void Close()
		{
			close(_file);
		}

		bool Open(const char* name)
		{
			_file = open(name, O_RDWR);
			return _file != -1;
		}

		DWORD Read(void* buffer, DWORD size)
		{
			return read(_file, buffer, size);
		}

		DWORD Write(void* buffer, DWORD size)
		{
			DWORD ret = write(_file, buffer, size);
		#ifdef __APPLE__
			// MACOSX doesn't support fdatasync
			fsync(_file);
		#else
			fdatasync(_file);
		#endif
			return ret;
		}

		void SetPointer(DWORD offset)
		{
			lseek(_file, offset, SEEK_SET);
		}

		DWORD GetFileSize()
		{
			struct stat s;

			fstat(_file, &s);

			return s.st_size;
		}

		void FlushMetadata()
		{
            fsync(_file);
		}
		
		bool IsValid()
		{
			return false;
			
		}
	};
#endif // _WIN32

	static const unsigned char LOGDB_UNINITIALIZED_BYTE = 0xEE;

	class PagedFile
	{
		DWORD _pageSize;
		DWORD _fileSize;
		File _file;

		typedef std::map<DWORD, unsigned char*> CacheMap;
		CacheMap _cache;

		typedef std::list<DWORD> CurrentCachePagesList;
		CurrentCachePagesList _currentCachePagesList;

		DWORD _cacheMaxPages;

		void* GetPage(DWORD page)
		{
			CacheMap::const_iterator i = _cache.find(page);

			if(i != _cache.end())
				return i->second;

			DWORD offset = page * _pageSize;
			unsigned char* buffer;

			//
			// cache full?
			//
			if(_currentCachePagesList.size() >= _cacheMaxPages)
			{
				CacheMap::iterator i = _cache.find(_currentCachePagesList.front());

				ASSERT(i != _cache.end());

				buffer = i->second;

				_currentCachePagesList.pop_front();
				_cache.erase(i);
			}
			else
				buffer = new unsigned char[_pageSize];

			_cache[page] = buffer;
			_currentCachePagesList.push_back(page);

			_file.SetPointer(offset);
			_file.Read(buffer, _pageSize);

			return buffer;
		}

		void FlushPage(DWORD page)
		{
			CacheMap::const_iterator i = _cache.find(page);

			if(i == _cache.end())
				return;

			DWORD offset = page * _pageSize;

			_file.SetPointer(offset);
			_file.Write(i->second, _pageSize);
		}

		DWORD PageByOffset(DWORD offset)
		{
			return offset / _pageSize;
		}
	public:
		PagedFile()
		{
			_pageSize = 4096;
			
			//
			// TODO: hardcoded cache size
			//
			_cacheMaxPages = (4 * 1042 * 1024) / _pageSize;
		}

		~PagedFile()
		{
			Close();
		}

		void FreeCache()
		{
			for(CacheMap::iterator i = _cache.begin() ; i != _cache.end() ; ++i)
				delete i->second;

			_cache.clear();
			_currentCachePagesList.clear();
		}

		DWORD GetFileSize()
		{
			return _file.GetFileSize();
		}

		void SetPageSize(DWORD pageSize)
		{
			FreeCache();
			_pageSize = pageSize;
		}

		DWORD GetPageSize()
		{
			return _pageSize;
		}

		bool Create(const char* name)
		{
			return _file.Create(name);
		}

		bool Open(const char* name)
		{
			return _file.Open(name);
		}

		bool IsValid()
		{
			return _file.IsValid();
		}

		void Close()
		{
			_file.Close();
			FreeCache();
		}

		template <class T>
		DWORD Read(DWORD offset, T& structBuffer)
		{
			return Read(offset, &structBuffer, sizeof(structBuffer));
		}

		template <class T>
		DWORD Write(DWORD offset, const T& structBuffer)
		{
			return Write(offset, &structBuffer, sizeof(structBuffer));
		}

		template <class T>
		DWORD Write(DWORD offset, T* structBuffer)
		{
			//
			// GCC doesn't like it, he'll validate this even if this template
			// is not used
#ifdef _MSC_VER
			int are_you_sure_you_want_to_write_a_pointer[-1];
#endif
		}

		DWORD Read(DWORD offset, void* buffer, DWORD size)
		{
			DWORD remaining = size;
			DWORD pageNumber = PageByOffset(offset);
			DWORD pageOffset = offset % _pageSize;
			unsigned char* ucharBuffer = (unsigned char*)buffer;

			while(remaining > 0)
			{
				unsigned char* pageBuffer = (unsigned char*)GetPage(pageNumber);
				DWORD toRead = min(remaining, _pageSize - pageOffset);

				pageBuffer += pageOffset;

				memcpy(ucharBuffer, pageBuffer, toRead);

				remaining -= toRead;

				if(remaining == 0)
					break;

				pageNumber++;
				ucharBuffer += toRead;
				pageOffset = 0;
			}

			return size - remaining;
		}

		DWORD Write(DWORD offset, const void* buffer, DWORD size)
		{
			DWORD remaining = size;
			DWORD pageNumber = PageByOffset(offset);
			DWORD pageOffset = offset % _pageSize;
			unsigned char* ucharBuffer = (unsigned char*)buffer;

			while(remaining > 0)
			{
				unsigned char* pageBuffer = (unsigned char*)GetPage(pageNumber);
				DWORD toWrite = min(remaining, _pageSize - pageOffset) ;

				pageBuffer += pageOffset;

				memcpy(pageBuffer, ucharBuffer, toWrite);

				FlushPage(pageNumber);

				remaining -= toWrite;

				if(remaining == 0)
					break;

				pageNumber++;
				ucharBuffer += toWrite;
				pageOffset = 0;
			}

			return size - remaining;
		}

		void GrowFile(DWORD pages)
		{
			DWORD bytes = pages * _pageSize;

			unsigned char* buffer = new unsigned char[bytes];
			memset(buffer, LOGDB_UNINITIALIZED_BYTE, bytes);

			_file.SetPointer(_file.GetFileSize());

			_file.Write(buffer, bytes);

			delete buffer;

			_file.FlushMetadata();
		}
	};

	struct LDB_FILE_HEADER
	{
		DWORD magic;
		DWORD version;
		DWORD flags;
		DWORD tableBlockOffset;
	};

	struct LDB_BLOCK_HEADER
	{
		DWORD size;
		DWORD usedCount;
		DWORD nextBlockOffset;
	};

	struct LDB_BLOCK_HEADER_INFO
	{
		LDB_BLOCK_HEADER blockHeader;
		DWORD offset;
	};

	struct LDB_LOG_RECORD_FIELD
	{
		DWORD dataOffset;
		DWORD dataSize;
		DWORD hash;
	};

	struct LDB_LOG_RECORD
	{
		DWORD operation;
		DWORD recordIndex;
		DWORD recordID;
		LDB_LOG_RECORD_FIELD key;
		LDB_LOG_RECORD_FIELD value;
		LDB_LOG_RECORD_FIELD metadata;
	};

	static const DWORD LDB_MAGIC = '*BDL';
	static const DWORD OPERATION_APPEND	= 1;
	static const DWORD OPERATION_INSERT = 2;
	static const DWORD OPERATION_SET = 3;
	static const DWORD OPERATION_DELETE	= 4;
	static const DWORD OPERATION_CLEAR	= 5;

	template<typename T>
	inline void zero(T& p)
	{
		memset(&p, 0, sizeof(p));
	}

	class LdbData
	{
		void* _buffer;
		bool _ownership;
		DWORD _size;
	public:
		enum SetType {copyBuffer, dontCopyBuffer};

		LdbData(const void* buffer, DWORD size, SetType copyOrNot = copyBuffer)
			: _buffer(NULL),
			_ownership(false),
			_size(0)
		{
			Set(buffer, size, copyOrNot);
		}

		LdbData() 
			: _buffer(NULL),
			_ownership(false),
			_size(0)
		{}

		void Free()
		{
			if(_ownership)
				free(_buffer);

			_ownership = false;
			_buffer = NULL;
			_size = 0;
		}

		~LdbData()
		{
			Free();
		}

		void Set(const void* buffer, DWORD size, SetType copyOrNot)
		{
			if(copyOrNot == dontCopyBuffer)
			{
				_buffer = const_cast<void*>(buffer);
				_ownership = false;
				_size = size;
			}
			else
			{
				memcpy(Alloc(size), buffer, size);
			}
		}

		void* Alloc(DWORD size)
		{
			Free();
			_size = size;
			_ownership = true;
			_buffer = malloc(size);
			return _buffer;

		}
		const void* GetBuffer() const
		{
			return _buffer;
		}

		DWORD GetSize() const
		{
			return _size;
		}
	};

	static const DWORD LDB_INVALID_RECNO = 0xFFFFFFFF;

	class Ldb
	{
	public:
		PagedFile _file;

		LDB_FILE_HEADER _header;

		DWORD _nextDataOffset;
		DWORD _growPagesStep;
		DWORD _defaultBlockSize;

		DWORD _totalRecordCount;
		DWORD _totalLogRecordCount;
		DWORD _lastRecordID;

		struct TABLE_INFO
		{
			typedef std::vector<LDB_LOG_RECORD> RecordsVector;

			std::string name;
			LDB_BLOCK_HEADER_INFO lastBlockHeaderInfo;
			RecordsVector records;
		};

		TABLE_INFO _metatable;

		typedef std::map< std::string, TABLE_INFO > TableMap;
		TableMap _tables;

		//
		// no copy
		//
		Ldb(const Ldb&);
		Ldb& operator=(const Ldb&);

		DWORD LoadAllBlocks(TABLE_INFO* tableInfo)
		{
			DWORD nextTableOffset = tableInfo->lastBlockHeaderInfo.offset;

			ASSERT(nextTableOffset);

			DWORD a;

			for(a = 0 ; nextTableOffset != 0 ;a++)
			{
				tableInfo->lastBlockHeaderInfo.offset = nextTableOffset;

				LoadBlock(tableInfo);

				ASSERT(tableInfo->lastBlockHeaderInfo.blockHeader.nextBlockOffset == 0 || 
					tableInfo->lastBlockHeaderInfo.blockHeader.nextBlockOffset > 
					nextTableOffset + tableInfo->lastBlockHeaderInfo.blockHeader.size);

				nextTableOffset = tableInfo->lastBlockHeaderInfo.blockHeader.nextBlockOffset;

			}

			return a;
		}

		void CheckUnitialized(DWORD offset, DWORD size)
		{
#ifdef _DEBUG
			unsigned char* buffer = new unsigned char[size];

			DWORD read = _file.Read(offset, buffer, size);
			
			for(DWORD a = 0 ; a < read ; a++)
				ASSERT(buffer[a] == 0xEE);

			delete buffer;
#endif //_DEBUG
		}

		void LoadTables()
		{
			_nextDataOffset = 0;
			_totalRecordCount = 0;
			_totalLogRecordCount = 0;

			LoadAllBlocks(&_metatable);

			for(TABLE_INFO::RecordsVector::iterator i = _metatable.records.begin() ; 
				i != _metatable.records.end() ;
				++i)
			{
				LDB_LOG_RECORD& record = *i;
				LdbData key;

				TABLE_INFO tableInfo;

				ASSERT(record.key.dataOffset != 0 && record.value.dataSize != 0);

				ReadField(&record.key, &key);
				ASSERT(key.GetSize());

				//
				// only fill the offset, LoadAllBlocks function will
				// load it
				//
				ASSERT(record.value.dataSize > sizeof(LDB_BLOCK_HEADER));
				tableInfo.lastBlockHeaderInfo.offset = record.value.dataOffset;

				tableInfo.name = std::string((char*)key.GetBuffer(), key.GetSize());

				LoadAllBlocks(&tableInfo);

				_tables[tableInfo.name] = tableInfo;
			}
			
			//
			// Can't check it here. If process crashes in the middle of a record write,
			// the last bytes will have parts of the written record (but it will not
			// be marked as valid). So, we can't assure the last bytes will be 0xEE
			//
			//CheckUnitialized(_nextDataOffset, 200);
		}

		bool LoadBlock(LDB_BLOCK_HEADER_INFO* blockHeaderInfo)
		{
			_file.Read(blockHeaderInfo->offset, blockHeaderInfo->blockHeader);
		}

		std::vector<std::string> GetTableList()
		{
			std::vector<std::string> ret;

			for(TableMap::const_iterator i = _tables.begin() ; i != _tables.end() ; ++i)
			{
				ret.push_back(i->first);
			}

			return ret;
		}


		TABLE_INFO* OpenTable(const std::string& name)
		{
			TableMap::iterator i = _tables.find(name);

			return i == _tables.end() ? NULL : &i->second;
		}

		void WriteBlockToFile(const LDB_BLOCK_HEADER_INFO& blockHeaderInfo)
		{
			ASSERT(blockHeaderInfo.offset && blockHeaderInfo.blockHeader.size 
				&& blockHeaderInfo.offset < _file.GetFileSize());

			_file.Write(blockHeaderInfo.offset, blockHeaderInfo.blockHeader);
		}


		bool InitializeFile()
		{	
			_header.magic = LDB_MAGIC;
			_header.version = 1;
			_header.flags = 0;
			_header.tableBlockOffset = sizeof(LDB_FILE_HEADER);

			_metatable.lastBlockHeaderInfo.offset = _header.tableBlockOffset;
			_metatable.lastBlockHeaderInfo.blockHeader.size = _defaultBlockSize;
			_metatable.lastBlockHeaderInfo.blockHeader.usedCount = 0;
			_metatable.lastBlockHeaderInfo.blockHeader.nextBlockOffset = 0;

			_nextDataOffset = _metatable.lastBlockHeaderInfo.offset +
				_metatable.lastBlockHeaderInfo.blockHeader.size;

			LeaveRoom(_nextDataOffset + _file.GetPageSize());

			//
			// we'll write the first block before the file header, so the file will not
			// be valid until we have one block
			//
			WriteBlockToFile(_metatable.lastBlockHeaderInfo);
			_file.Write(0, _header);

			return true;
		}

		std::auto_ptr<LDB_LOG_RECORD> LoadLogRecords(const LDB_BLOCK_HEADER_INFO& blockHeaderInfo)
		{
			if(blockHeaderInfo.blockHeader.usedCount == 0)
				return std::auto_ptr<LDB_LOG_RECORD>();

			DWORD recordBufferSize = blockHeaderInfo.blockHeader.usedCount * sizeof(LDB_LOG_RECORD);
			std::auto_ptr<LDB_LOG_RECORD> logRecords(new LDB_LOG_RECORD[blockHeaderInfo.blockHeader.usedCount]);

			memset(logRecords.get(), 0, recordBufferSize);

			_file.Read(blockHeaderInfo.offset +  sizeof(LDB_BLOCK_HEADER), logRecords.get(), recordBufferSize);

			return logRecords;
		}

		bool DeleteTable(TABLE_INFO* tableInfo)
		{
			DWORD index = static_cast<DWORD>(FindKey(&_metatable, 0, 
				LdbData(tableInfo->name.c_str(), static_cast<DWORD>(tableInfo->name.size()), LdbData::dontCopyBuffer)));

			if(index == LDB_INVALID_RECNO)
				return false;
			
			DeleteByIndex(&_metatable, index);

			_tables.erase(tableInfo->name);

			return true;
		}

		TABLE_INFO* CreateTable(const std::string& name)
		{
			TableMap::iterator i = _tables.find(name);

			if(i != _tables.end())
			{
				ASSERT(FindKey(&_metatable, 0, 
					LdbData(name.c_str(), (DWORD)name.size(), LdbData::dontCopyBuffer)) != LDB_INVALID_RECNO);
				return &i->second;
			}
			else
			{
				ASSERT(FindKey(&_metatable, 0, 
					LdbData(name.c_str(), (DWORD)name.size(), LdbData::dontCopyBuffer)) == LDB_INVALID_RECNO);
			}

			TABLE_INFO& tableInfo = _tables[name];

			tableInfo.name = name;
			tableInfo.lastBlockHeaderInfo.blockHeader.nextBlockOffset = 0;
			tableInfo.lastBlockHeaderInfo.blockHeader.size = _defaultBlockSize;
			tableInfo.lastBlockHeaderInfo.blockHeader.usedCount = 0;

			std::auto_ptr<unsigned char> buffer(new unsigned char[_defaultBlockSize]);
			memset(buffer.get(), LOGDB_UNINITIALIZED_BYTE, _defaultBlockSize);
			*((LDB_BLOCK_HEADER*)buffer.get()) = tableInfo.lastBlockHeaderInfo.blockHeader;

			LdbData key(name.c_str(), (DWORD)name.size(), LdbData::dontCopyBuffer);
			LdbData value(buffer.get(), _defaultBlockSize, LdbData::dontCopyBuffer);

			DWORD index = Append(&_metatable, &key, &value, NULL);
			ASSERT(index < _metatable.records.size());

			tableInfo.lastBlockHeaderInfo.offset = _metatable.records[index].value.dataOffset;
			ASSERT(tableInfo.lastBlockHeaderInfo.offset > 0 && tableInfo.lastBlockHeaderInfo.offset < _file.GetFileSize());

			return &tableInfo;
		}

		

		void LoadBlock(TABLE_INFO* tableInfo)
		{
			//
			// read block to memory
			//
			_file.Read(tableInfo->lastBlockHeaderInfo.offset, tableInfo->lastBlockHeaderInfo.blockHeader);

			DWORD afterBlockHeaderOffset = 
				tableInfo->lastBlockHeaderInfo.offset + tableInfo->lastBlockHeaderInfo.blockHeader.size;
			
			if(afterBlockHeaderOffset > _nextDataOffset)
				_nextDataOffset = afterBlockHeaderOffset;

			if(tableInfo->lastBlockHeaderInfo.blockHeader.usedCount == 0)
				return;
			//
			// read log records to memory
			//
			std::auto_ptr<LDB_LOG_RECORD> logRecords = LoadLogRecords(tableInfo->lastBlockHeaderInfo);

			LDB_LOG_RECORD* logRecord = NULL;

			//
			// do the action specified by the log record
			//
			for(DWORD a = 0 ; a < tableInfo->lastBlockHeaderInfo.blockHeader.usedCount ; a++)
			{
				logRecord = &logRecords.get()[a];
				_totalLogRecordCount++;

				switch(logRecord->operation)
				{
				case OPERATION_APPEND:
					_totalRecordCount++;
					tableInfo->records.push_back(*logRecord);
					break;

				case OPERATION_SET:
					if(logRecord->recordIndex < tableInfo->records.size())
						tableInfo->records[logRecord->recordIndex] = *logRecord;
					break;

				case OPERATION_CLEAR:
					_totalRecordCount -= tableInfo->records.size();
					tableInfo->records.clear();
					break;

				case OPERATION_INSERT:
					if(logRecord->recordIndex == tableInfo->records.size())
					{
						_totalRecordCount++;
						tableInfo->records.push_back(*logRecord);
					}
					if(logRecord->recordIndex < tableInfo->records.size())
					{
						tableInfo->records.insert(tableInfo->records.begin() + logRecord->recordIndex, *logRecord);
						_totalRecordCount++;
					}
					break;

				case OPERATION_DELETE:
					if(logRecord->recordIndex < tableInfo->records.size())
					{
						tableInfo->records.erase(tableInfo->records.begin() + logRecord->recordIndex);
						_totalRecordCount--;
					}
					break;
				default:
					ASSERT(false);
				}

				//
				// is this the last record?

				if(logRecord->recordID > _lastRecordID)
					_lastRecordID = logRecord->recordID;

				DWORD next = 0;

				//
				// find the last written field, to calculate how many bytes
				// we must walk until the next record
				//
				if(logRecord->key.dataOffset > logRecord->value.dataOffset)
					if(logRecord->key.dataOffset > logRecord->metadata.dataOffset)
						next = logRecord->key.dataOffset + logRecord->key.dataSize;
					else
						next = logRecord->metadata.dataOffset + logRecord->metadata.dataSize;
				else
					if(logRecord->value.dataOffset > logRecord->metadata.dataOffset)
						next = logRecord->value.dataOffset + logRecord->value.dataSize;
					else
						next = logRecord->metadata.dataOffset + logRecord->metadata.dataSize;

				ASSERT(
					logRecord->operation == OPERATION_DELETE ||
					logRecord->operation == OPERATION_CLEAR  ||
					next != 0);

				if(next > _nextDataOffset)
					_nextDataOffset = next;				
			}

			return;
		}


		bool LoadFile()
		{
			Clear();

			//
			// read file header
			//
			_file.Read(0, &_header, sizeof(_header));

			if(_header.magic != LDB_MAGIC)
			{
				ASSERT(false);
				_file.Close();
				return false;
			}

			_metatable.lastBlockHeaderInfo.offset = _header.tableBlockOffset;

			DWORD currentPageSize = _file.GetPageSize();

			//
			// using 4MB page size for load will make it *much* faster
			//
			_file.SetPageSize(1024 * 1024 * 4);

			LoadTables();

			_file.SetPageSize(currentPageSize);

			return true;
		}

		DWORD DoHash(const LdbData& data)
		{
			return DoHash(data.GetBuffer(), data.GetSize());
		}

		DWORD DoHash(const void* buf, DWORD size)
		{
			DWORD hash = 2;
			unsigned char* buffer = (unsigned char*) buf;

			for(DWORD a = 0 ; a < size ; a++)
				hash ^= buffer[a];

			return hash;
		}

		void Clear()
		{
			zero(_header);
			zero(_metatable.lastBlockHeaderInfo);

			_metatable.records.clear();
			_tables.clear();
			_nextDataOffset = 0;
			_totalRecordCount = 0;
			_totalLogRecordCount = 0;
			_lastRecordID = 0;

		}

		void LeaveRoom(DWORD size)
		{
			DWORD pages = (size / _file.GetPageSize()) + 1;

			if(pages < _growPagesStep)
				pages = _growPagesStep;

			_file.GrowFile(pages);
		}

		void EnsureSpace(DWORD size)
		{
			DWORD space = _file.GetFileSize() - _nextDataOffset;

			if(space < size)
				LeaveRoom(size - space);
		}

		void WriteLdbData(const LdbData* ldbData, LDB_LOG_RECORD_FIELD* logRecordField)
		{
			if(!ldbData)
				return;

			logRecordField->dataOffset = _nextDataOffset;
			logRecordField->dataSize = ldbData->GetSize();
			logRecordField->hash = DoHash(*ldbData);

			EnsureSpace(logRecordField->dataSize);

			ASSERT(_nextDataOffset && _nextDataOffset < _file.GetFileSize());

			DWORD size = ldbData->GetSize();

			_file.Write(_nextDataOffset, ldbData->GetBuffer(), size);

			_nextDataOffset += logRecordField->dataSize;
		}

		bool AppendLogRecord(TABLE_INFO* tableInfo, DWORD operation, DWORD recordIndex, 
			const LdbData* key, const LdbData* value, const LdbData* metadata)
		{
			LDB_LOG_RECORD logRecord;
			LDB_BLOCK_HEADER_INFO* blockHeaderInfo = &tableInfo->lastBlockHeaderInfo;
			TABLE_INFO::RecordsVector& records = tableInfo->records;

			ASSERT(
				operation == OPERATION_CLEAR || 
				(operation == OPERATION_INSERT && recordIndex == 0) ||
				operation == OPERATION_APPEND || 
				CheckIndex(tableInfo, recordIndex));

			zero(logRecord);

			logRecord.operation = operation;
			logRecord.recordIndex = recordIndex;
			logRecord.recordID = ++_lastRecordID;

			LDB_BLOCK_HEADER_INFO newBlockHeaderInfo;
			bool hasNewBlock;

			hasNewBlock = CreateNewBlockIfNeeded(blockHeaderInfo, &newBlockHeaderInfo);

			if(key)
				WriteLdbData(key, &logRecord.key);

			if(value)
				WriteLdbData(value, &logRecord.value);

			if(metadata)
				WriteLdbData(metadata, &logRecord.metadata);

			if(operation == OPERATION_SET)
			{
				//
				// caller can keep fields passing NULL
				//
				LDB_LOG_RECORD& setRecord = records[recordIndex];

				if(key == NULL)
					logRecord.key = setRecord.key;
				if(value == NULL)
					logRecord.value = setRecord.value;
				if(metadata== NULL)
					logRecord.metadata = setRecord.metadata;
			}

			LDB_BLOCK_HEADER_INFO* currentBlockHeaderInfo = hasNewBlock ? &newBlockHeaderInfo : blockHeaderInfo;

			DWORD logRecordOffset = currentBlockHeaderInfo->offset + sizeof(LDB_BLOCK_HEADER) +
				(sizeof(LDB_LOG_RECORD) * currentBlockHeaderInfo->blockHeader.usedCount);

			currentBlockHeaderInfo->blockHeader.usedCount++;

			ASSERT(logRecordOffset && currentBlockHeaderInfo->offset &&
				logRecordOffset < _file.GetFileSize() && currentBlockHeaderInfo->offset < _file.GetFileSize());

			CheckUnitialized(logRecordOffset, sizeof(logRecord));
			_file.Write(logRecordOffset, logRecord);

			_file.Write(currentBlockHeaderInfo->offset, currentBlockHeaderInfo->blockHeader);

			if(hasNewBlock)
			{
				ASSERT(blockHeaderInfo->offset && blockHeaderInfo->offset < _file.GetFileSize());

				_file.Write(blockHeaderInfo->offset, blockHeaderInfo->blockHeader);

				*blockHeaderInfo = newBlockHeaderInfo;
			}

			switch(operation)
			{
			case OPERATION_APPEND:
				_totalRecordCount++;
				records.push_back(logRecord);
				break;
			case OPERATION_SET:
				records[recordIndex] = logRecord;
				break;
			case OPERATION_INSERT:
				_totalRecordCount++;
				if(recordIndex == records.size())
					records.push_back(logRecord);
				else
					records.insert(records.begin() + recordIndex, logRecord);
				break;
			case OPERATION_DELETE:
				_totalRecordCount--;
				records.erase(records.begin() + recordIndex);
				break;
			case OPERATION_CLEAR:
				_totalRecordCount-= records.size();
				records.clear();
				break;
			}

			_totalLogRecordCount++;

			return true;
		}

		DWORD CalculateAddedFieldPos(const LdbData* data, LDB_LOG_RECORD_FIELD* field)
		{
			if(!data || data->GetSize() == 0)
			{
				field->dataOffset = 0;
				field->dataSize = 0;
				field->hash = 0;
				return 0;
			}

			field->dataOffset = _nextDataOffset;
			field->dataSize = data->GetSize();
			field->hash = DoHash(*data);
			_nextDataOffset += field->dataSize;

			return field->dataSize;
		}

		void WriteLogRecordFieldData(LDB_LOG_RECORD_FIELD* field, const void* buffer)
		{
			if(!buffer || field->dataSize == 0)
				return;

			ASSERT(field->dataOffset != 0 && field->dataOffset < _file.GetFileSize());

			_file.Write(field->dataOffset, buffer, field->dataSize);

			_nextDataOffset += field->dataSize;
		}

		bool CheckNeedForNewBlock(const LDB_BLOCK_HEADER& currentBlock)
		{
			DWORD nextRecordOffset = sizeof(LDB_FILE_HEADER) + (currentBlock.usedCount * sizeof(LDB_LOG_RECORD));
			DWORD spaceLeftInThisBlock = currentBlock.size - nextRecordOffset;
			return spaceLeftInThisBlock < sizeof(LDB_LOG_RECORD);
		}


		bool CreateNewBlockIfNeeded(LDB_BLOCK_HEADER_INFO* currentBlockHeaderInfo, LDB_BLOCK_HEADER_INFO* newBlockHeaderInfo)
		{
			if(!CheckNeedForNewBlock(currentBlockHeaderInfo->blockHeader))
				return false;

			LDB_BLOCK_HEADER* currentBlock = &currentBlockHeaderInfo->blockHeader;
			LDB_BLOCK_HEADER* newBlock = &newBlockHeaderInfo->blockHeader;

			EnsureSpace(currentBlock->size);

			newBlock->nextBlockOffset = 0;
			newBlock->size = currentBlock->size;
			newBlock->usedCount = 0;

			ASSERT(_nextDataOffset && _nextDataOffset < _file.GetFileSize());

			CheckUnitialized(_nextDataOffset, sizeof(*newBlock));
			_file.Write(_nextDataOffset, *newBlock);

			newBlockHeaderInfo->offset = _nextDataOffset;
			currentBlock->nextBlockOffset = newBlockHeaderInfo->offset;

			_nextDataOffset += newBlock->size;

			return true;
		}

		void ReadField(const LDB_LOG_RECORD_FIELD* field, LdbData* data)
		{
			ASSERT(field && data);

			_file.Read(field->dataOffset, data->Alloc(field->dataSize), field->dataSize);
		}


	public:

		Ldb()
		{
			_totalRecordCount = 0;
			_defaultBlockSize = 4096;
			_growPagesStep = (4 * 1024 * 1024) / _file.GetPageSize();
			_lastRecordID = 0;
		}

		~Ldb()
		{
			Close();
		}

		bool Create(const char* fileName)
		{
			bool b = _file.Create(fileName);

			if(!b)
				return false;

			if(_file.GetFileSize() < sizeof(LDB_FILE_HEADER))
				return InitializeFile();
			else
				return LoadFile();
		}

		bool Open(const char* fileName)
		{
			bool b = _file.Open(fileName);

			if(!b)
				return false;

			return LoadFile();
		}

		void Close()
		{
			_file.Close();
			Clear();
		}

		DWORD GetRecordCount(TABLE_INFO* tableInfo)
		{
			return static_cast<DWORD>(tableInfo->records.size());
		}

		bool CheckIndex(TABLE_INFO* tableInfo, DWORD index)
		{
			return index < tableInfo->records.size();
		}

		DWORD FindKey(TABLE_INFO* tableInfo, DWORD startIndex, const LdbData& key)
		{
			if(!CheckIndex(tableInfo, startIndex))
				return LDB_INVALID_RECNO;

			DWORD recordCount = static_cast<DWORD>(tableInfo->records.size());
			DWORD keyHash = DoHash(key);

			for(DWORD a = startIndex ; a < recordCount ; a++)
			{
				LDB_LOG_RECORD& logRecord = tableInfo->records[a];

				if(logRecord.key.dataSize != key.GetSize() || logRecord.key.hash != keyHash)
					continue;

				LdbData keyFromDb;

				DWORD index = GetByIndex(tableInfo, a, &keyFromDb, NULL, NULL);

				ASSERT(index == a && keyFromDb.GetSize() == key.GetSize());

				// should not happen
				if(keyFromDb.GetSize() != key.GetSize())
					continue;

				bool isEqual = (memcmp(key.GetBuffer(), keyFromDb.GetBuffer(), key.GetSize()) == 0);

				if(isEqual)
					return a;
			}

			return LDB_INVALID_RECNO;
		}

		DWORD Append(TABLE_INFO* tableInfo, const LdbData* key, const LdbData* value, const LdbData* metadata)
		{
			DWORD index = static_cast<DWORD>(tableInfo->records.size());

			bool b = AppendLogRecord(tableInfo, OPERATION_APPEND, index, key, value, metadata);

			if(!b)
			{
				ASSERT(index == static_cast<DWORD>(tableInfo->records.size()));
				return LDB_INVALID_RECNO;
			}

			return index;
		}

		void ClearAllRecords(TABLE_INFO* tableInfo)
		{
			bool b = AppendLogRecord(tableInfo, OPERATION_CLEAR, 0, NULL, NULL, NULL);

			ASSERT(b);
		}

		DWORD Set(TABLE_INFO* tableInfo, DWORD startIndex, const LdbData& key, const LdbData* value, const LdbData* metadata)
		{
			DWORD index = FindKey(tableInfo, startIndex, key);

			if(index == LDB_INVALID_RECNO)
				return Append(tableInfo, &key, value, metadata);
			else
				return SetByIndex(tableInfo, index, &key, value, metadata);
		}

		DWORD SetByIndex(TABLE_INFO* tableInfo, DWORD index, const LdbData* key, const LdbData* value, const LdbData* metadata)
		{
			if(!CheckIndex(tableInfo, index))
				return LDB_INVALID_RECNO;

			bool b = AppendLogRecord(tableInfo, OPERATION_SET, index, key, value, metadata);

			if(!b)
				return LDB_INVALID_RECNO;

			return index;
		}

		DWORD Delete(TABLE_INFO* tableInfo, DWORD startIndex, const LdbData& key)
		{
			DWORD index = FindKey(tableInfo, startIndex, key);

			if(index == LDB_INVALID_RECNO)
				return index;

			return DeleteByIndex(tableInfo, index);
		}

		DWORD DeleteByIndex(TABLE_INFO* tableInfo, DWORD index)
		{
			if(!CheckIndex(tableInfo, index))
				return LDB_INVALID_RECNO;

			bool b = AppendLogRecord(tableInfo, OPERATION_DELETE, index, NULL, NULL, NULL);

			if(!b)
				return LDB_INVALID_RECNO;

			return index;
		}

		DWORD InsertByIndex(TABLE_INFO* tableInfo, DWORD index, LdbData* key, LdbData* value, LdbData* metadata)
		{
			if(index != 0 && !CheckIndex(tableInfo, index))
				return LDB_INVALID_RECNO;

			bool b = AppendLogRecord(tableInfo, OPERATION_INSERT, index, key, value, metadata);

			if(!b)
				return LDB_INVALID_RECNO;

			return index;
		}


		void GetRecordSizes(TABLE_INFO* tableInfo, DWORD index, DWORD* keySize, DWORD* valueSize, DWORD* metadataSize)
		{
			if(index + 1 < tableInfo->records.size())
				return;

			if(keySize)
				*keySize = tableInfo->records[index].key.dataSize;

			if(valueSize)
				*valueSize = tableInfo->records[index].value.dataSize;

			if(metadataSize)
				*metadataSize = tableInfo->records[index].metadata.dataSize;
		}

		DWORD GetByIndex(TABLE_INFO* tableInfo, DWORD index, LdbData* key, LdbData* value, LdbData* metadata)
		{
			if(index + 1 > tableInfo->records.size())
				return LDB_INVALID_RECNO;

			const LDB_LOG_RECORD& logRecord = tableInfo->records[index];

			if(key && logRecord.key.dataSize)
				ReadField(&logRecord.key, key);
			if(value && logRecord.value.dataSize)
				ReadField(&logRecord.value, value);
			if(metadata && logRecord.metadata.dataSize)
				ReadField(&logRecord.metadata, metadata);

			return index;
		}

		DWORD Get(TABLE_INFO* tableInfo, DWORD startIndex, const LdbData& key, LdbData* value, LdbData* metadata)
		{
			DWORD index = FindKey(tableInfo, startIndex, key);

			if(index == LDB_INVALID_RECNO)
				return index;

			index = GetByIndex(tableInfo, index, NULL, value, metadata);

			ASSERT(index != LDB_INVALID_RECNO);

			return index;
		}

		DWORD GetPageSize()
		{
			return _file.GetPageSize();
		}

		DWORD GetBlockSize()
		{
			return _defaultBlockSize;
		}

		DWORD GetGrowStep()
		{
			return _growPagesStep * GetPageSize();
		}

		void SetGrowStep(DWORD bytes)
		{
			DWORD pageSize = GetPageSize();
			_growPagesStep = (bytes / pageSize) + 1;
		}
	};
	
} // namespace logdb

