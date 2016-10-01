
#include "TioTcpClient.h"

namespace tio
{
	RemoteContainerManager::RemoteContainerManager(asio::io_service& io_service):
		io_service_(io_service),
		socket_(io_service),
		response_stream_(&response_)
	{
	}

	void RemoteContainerManager::Connect(const char* host, unsigned short port)
	{
		tcp::resolver resolver(io_service_);
		tcp::resolver::query query(host, lexical_cast<string>(port));
		tcp::resolver::iterator i = resolver.resolve(query);
		
		if(i == tcp::resolver::iterator())
			throw std::runtime_error("couldn't resolve server name");

		socket_.connect(*i);
	}

	string RemoteContainerManager::ReceiveLine()
	{
		//
		// read_until is "read as much as you want, until you find '\n'"
		// and not "just read until '\n'.
		// So it can read more than one line
		//

		for(;;)
		{
			const char* begin = asio::buffer_cast<const char*>(response_.data());
			const char* i = begin;
			const char* end = begin + asio::buffer_size(response_.data());		
			
			for(; i != end ; ++i)
			{
				if(*i == '\n')
					break;
			}

			//
			// no \n ? we need more data
			//
			if(i == end)
			{
				asio::read_until(socket_, response_, '\n');
				continue;
			}

			const char* stringEnd = i;
			
			if(i != begin && *(i -1) == '\r')
				--stringEnd;

			string ret(begin, stringEnd);

			response_.consume(i - begin + 1);

			return ret;
		}

	}

	void RemoteContainerManager::ReceiveDataAnswer(vector<string>::iterator begin, vector<string>::iterator end, ProtocolAnswer* answer)
	{
		size_t fieldsTotalSize;
		vector<FieldInfo> fields;

		pair_assign(fields, fieldsTotalSize) = ExtractFieldSet(begin, end);

		if(response_.size() < fieldsTotalSize)
			asio::read(socket_, response_, asio::transfer_at_least(fieldsTotalSize - response_.size()));

		BOOST_ASSERT(response_.size() >= fieldsTotalSize);

		ExtractFieldsFromBuffer(
			fields, 
			asio::buffer_cast<const void*>(response_.data()),
			fieldsTotalSize,
			&answer->key, &answer->value, &answer->metadata);

		response_.consume(fieldsTotalSize);

		return;
	}


	void RemoteContainerManager::HandleEventAnswer(ProtocolAnswer* answer)
	{
		BOOST_ASSERT(answer->type == ProtocolAnswer::TypeEvent);

		Event e;
		e.name = answer->eventName;
		e.handle = lexical_cast<unsigned int>(answer->parameter);
		e.key = answer->key;
		e.value = answer->value;
		e.metadata = answer->metadata;

		pendingEvents_.push(e);
	}
		
	unsigned int RemoteContainerManager::Subscribe(unsigned int handle, EventSink sink, const string& start)
	{
		ProtocolAnswer answer;
		string param = lexical_cast<string>(handle);

		if(!start.empty())
		{
			param += " ";
			param += start;
		}
		
		SendCommand("subscribe", param, &answer);
		
		if(answer.error)
			throw std::runtime_error(answer.parameter);

		return dispatchers_[handle].Subscribe(sink);
	}

	void RemoteContainerManager::WaitAndPop_Next(shared_ptr<ITioContainer> container, EventSink sink)
	{
		RemoteContainer* remoteContainer = dynamic_cast<RemoteContainer*>(container.get());

		if(!remoteContainer)
			throw std::runtime_error("data container not a remote data container");

		ProtocolAnswer answer;

		const string& handle = remoteContainer->GetHandle();

		SendCommand("wnp_next", handle, &answer);

		if(answer.error)
			throw std::runtime_error(answer.parameter);

		poppers_[lexical_cast<unsigned int>(handle)].push(sink);
	}


	void RemoteContainerManager::WaitAndPop_Key(shared_ptr<ITioContainer> container, EventSink sink, const TioData& key)
	{
		RemoteContainer* remoteContainer = dynamic_cast<RemoteContainer*>(container.get());

		if(!remoteContainer)
			throw std::runtime_error("data container not a remote data container");

		ProtocolAnswer answer;

		const string& handle = remoteContainer->GetHandle();

		SendCommand("wnp_key", handle, &answer, key);

		if(answer.error)
			throw std::runtime_error(answer.parameter);

		keyPoppers_[lexical_cast<unsigned int>(handle)][key.AsSz()].push(sink);

	}

	void RemoteContainerManager::Unsubscribe(unsigned int handle, unsigned int cookie)
	{
		ProtocolAnswer answer;

		SendCommand("unsubscribe", lexical_cast<string>(handle), &answer);

		if(answer.error)
			throw std::runtime_error(answer.parameter);

		dispatchers_[handle].Unsubscribe(cookie);

	}

	void RemoteContainerManager::ReceiveAndDispatchEvents()
	{
		ProtocolAnswer answer;

		//
		// note it will block if there's no events
		//

		if(pendingEvents_.size() == 0)
		{
			Receive(&answer);
			BOOST_ASSERT(answer.type == ProtocolAnswer::TypeEvent);
			HandleEventAnswer(&answer);
		}
		
		while(pendingEvents_.size())
		{
			const Event& e = pendingEvents_.front();

			if(e.name == "wnp_next")
			{
				PoppersMap::iterator i = poppers_.find(e.handle);
				ASSERT(i != poppers_.end());
				if(i != poppers_.end())
				{
					queue<EventSink>& poppersQueue = i->second;

					if(!poppersQueue.empty())
					{
						poppersQueue.front()(e.name, e.key, e.value, e.metadata);
						poppersQueue.pop();
					}
				}
			}
			else if(e.name == "wnp_key")
			{
				KeyPoppersMap::iterator i = keyPoppers_.find(e.handle);
				if(i != keyPoppers_.end() && e.key.GetDataType() == TioData::Sz)
				{
					std::map< std::string, queue<EventSink> >& sinksPerKey = i->second;
					std::map< std::string, queue<EventSink> >::iterator i2 = sinksPerKey.find(e.key.AsSz());
					
					if(i2 != i->second.end())
					{
						queue<EventSink>& poppersQueue = i2->second;

						if(!poppersQueue.empty())
						{
							poppersQueue.front()(e.name, e.key, e.value, e.metadata);
							poppersQueue.pop();
						}

					}
				}
			}
			else
			{
				dispatchers_[e.handle].RaiseEvent(e.name, e.key, e.value, e.metadata);
			}
			
			pendingEvents_.pop();
		}
	}

	void RemoteContainerManager::ReceiveUntilAnswer(ProtocolAnswer* answer)
	{
		for(;;)
		{
			Receive(answer);

			if(answer->type == ProtocolAnswer::TypeAnswer)
				break;

			HandleEventAnswer(answer);
		}
	}

	void RemoteContainerManager::Receive(ProtocolAnswer* answer)
	{	
		string answerLine = ReceiveLine();

		ParseAnswerLine(answerLine, answer);

		if(answer->pendingDataSize == 0)
			return;

		if(response_.size() < answer->pendingDataSize)
			asio::read(socket_, response_, asio::transfer_at_least(answer->pendingDataSize - response_.size()));

		BOOST_ASSERT(response_.size() >= answer->pendingDataSize);

		ExtractFieldsFromBuffer(
			answer->fieldSet, 
			asio::buffer_cast<const void*>(response_.data()),
			answer->pendingDataSize,
			&answer->key, &answer->value, &answer->metadata);

		response_.consume(answer->pendingDataSize);
	}

	inline void RemoteContainerManager::SendCommand(const string& command, const string parameter, ProtocolAnswer* answer,
													const TioData& key, const TioData& value, const TioData& metadata)
	{
		stringstream cmdStream;

		cmdStream << command;

		if(!parameter.empty())
			cmdStream << " " << parameter; 

		if(!key && !value && !metadata)
			cmdStream << "\r\n";
		else
			SerializeData(key, value, metadata, cmdStream);

		asio::write(socket_, asio::buffer(cmdStream.str()));

		if(answer)
			ReceiveUntilAnswer(answer);

		return;
	}

	shared_ptr<ITioContainer> RemoteContainerManager::CreateOrOpenContainer(
		const string& command ,const string& type, const string& name)
	{
		ProtocolAnswer answer;
		
		SendCommand(command, name + " " + type, &answer);

		if(answer.error)
			throw std::runtime_error(answer.parameter);

		if(answer.parameterType != "handle" || answer.parameter.empty())
			throw std::runtime_error("invalid answer from server");

		RemoteContainer* remoteContainer = new RemoteContainer(*this, answer.parameter);
		
		return shared_ptr<ITioContainer>(remoteContainer);
	}

	shared_ptr<ITioContainer> RemoteContainerManager::CreateContainer(const string& type, const string& name)
	{
		return CreateOrOpenContainer("create", type, name);
	}

	shared_ptr<ITioContainer> RemoteContainerManager::OpenContainer(const string& type, const string& name)
	{
		return CreateOrOpenContainer("open", type, name);
	}



	RemoteContainer::RemoteContainer(RemoteContainerManager& manager, string handle) :
	manager_(manager),
	handle_(handle)
	{}

	RemoteContainer::~RemoteContainer()
	{
		return;
	}

	inline void RemoteContainer::ThrowAnswerIfError(const ProtocolAnswer& answer)
	{
		if(answer.error)
			throw std::runtime_error(answer.parameter);
	}

	inline void RemoteContainer::SendCommand(const string& command, ProtocolAnswer* answer, 
											   const TioData& key, const TioData& value, const TioData& metadata)
	{
		manager_.SendCommand(command, handle_, answer, key, value, metadata);

		if(answer)
			ThrowAnswerIfError(*answer);
	}

	inline void RemoteContainer::SendDataCommand(const string& command, 
		TioData* key, TioData* value, TioData* metadata)
	{
		ProtocolAnswer answer;
		SendCommand(command, &answer, 
			key ? *key : TIONULL, 
			value ? *value : TIONULL,
			metadata ? *metadata : TIONULL);

		if(key && answer.key)
			*key = answer.key;
		
		if(value && answer.value)
			*value = answer.value;

		if(metadata && answer.metadata)
			*metadata = answer.metadata;
	}

	inline void RemoteContainer::SendOutputDataCommand(const string& command, 
		TioData* key, TioData* value, TioData* metadata)
	{
		ProtocolAnswer answer;
		SendCommand(command, &answer, 
			TIONULL, 
			TIONULL,
			TIONULL);

		if(key && answer.key)
			*key = answer.key;

		if(value && answer.value)
			*value = answer.value;

		if(metadata && answer.metadata)
			*metadata = answer.metadata;
	}

	inline void RemoteContainer::SendDataCommand(const string& command, 
		const TioData& key, TioData* value, TioData* metadata)
	{
		ProtocolAnswer answer;
		SendCommand(command, &answer, 
			key, 
			value ? *value : TIONULL,
			metadata ? *metadata : TIONULL);

		if(value && answer.value)
			*value = answer.value;

		if(metadata && answer.metadata)
			*metadata = answer.metadata;
	}

	inline void RemoteContainer::SendDataCommand(const string& command, 
		const TioData& key, const TioData& value, const TioData& metadata)
	{
		ProtocolAnswer answer;
		
		SendCommand(command, &answer, key, value, metadata);
	}


	size_t RemoteContainer::GetRecordCount()
	{
		ProtocolAnswer answer;

		SendCommand("get_count", &answer);

		BOOST_ASSERT(answer.parameterType == "count");

		return lexical_cast<size_t>(answer.parameter);
	}

	string RemoteContainer::GetType()
	{
		ProtocolAnswer answer;

		SendCommand("get_type", &answer);

		BOOST_ASSERT(answer.parameterType == "type");

		return answer.parameter;
	}
	string RemoteContainer::GetName()
	{
		ProtocolAnswer answer;

		SendCommand("get_name", &answer);

		BOOST_ASSERT(answer.parameterType == "name");

		return answer.parameter;
	}

	string RemoteContainer::Command(const string& command)
	{
		ProtocolAnswer answer;

		SendCommand("command", &answer);

		return answer.parameter;
	}

	void RemoteContainer::PushBack(const TioData& key, const TioData& value, const TioData& metaData)
	{
		SendDataCommand("push_back", key, value, metaData);
	}
	void RemoteContainer::PushFront(const TioData& key, const TioData& value, const TioData& metaData)
	{
		SendDataCommand("push_front", key, value, metaData);
	}

	void RemoteContainer::PopBack(TioData* key, TioData* value, TioData* metaData)
	{
		SendOutputDataCommand("pop_back", key, value, metaData);
	}
	void RemoteContainer::PopFront(TioData* key, TioData* value, TioData* metaData)
	{
		SendOutputDataCommand("pop_front", key, value, metaData);
	}

	void RemoteContainer::GetRecord(const TioData& searchKey, TioData* key,  TioData* value, TioData* metaData)
	{
		TioData keyCopy = searchKey;
		
		SendDataCommand("get", key, value, metaData);
		
		if(key)
			*key = keyCopy;
	}

	void RemoteContainer::Set(const TioData& key, const TioData& value, const TioData& metaData)
	{
		SendDataCommand("set", key, value, metaData);
	}

	void RemoteContainer::Insert(const TioData& key, const TioData& value, const TioData& metaData)
	{
		SendDataCommand("insert", key, value, metaData);
	}

	void RemoteContainer::Modify(const TioData& key, TioData* value)
	{
		SendDataCommand("modify", key, value, NULL);
	}

	void RemoteContainer::Delete(const TioData& key, const TioData& value, const TioData& metadata)
	{
		SendDataCommand("delete", key, value, metadata);
	}

	void RemoteContainer::Clear()
	{
		ProtocolAnswer answer;
		
		SendCommand("clear", &answer, TIONULL, TIONULL, TIONULL);

		if(answer.error)
			throw std::runtime_error(answer.parameter);
	}

	shared_ptr<ITioResultSet> RemoteContainer::Query(int startOffset, int endOffset, const TioData& query)
	{
		throw std::runtime_error("not implemented");
	}

	void RemoteContainer::SetProperty(const string& key, const string& value)
	{
		SendDataCommand("set_property", &TioData(key), &TioData(value));
	}
	string RemoteContainer::GetProperty(const string& key)
	{
		TioData value;
		ProtocolAnswer answer;
		
		SendCommand("get_property", &answer, key, TIONULL, TIONULL);
		
		if(answer.error)
			throw std::runtime_error(answer.errorMessage.c_str());

		if(answer.value.GetDataType() != TioData::Sz)
			throw std::runtime_error("invalid answer from server");

		return answer.value.AsSz();
	}

	unsigned int RemoteContainer::Subscribe(EventSink sink, const string& start)
	{
		return manager_.Subscribe(lexical_cast<unsigned int>(handle_), sink, start);
	}
	void RemoteContainer::Unsubscribe(unsigned int cookie)
	{
		manager_.Unsubscribe(lexical_cast<unsigned int>(handle_), cookie);
	}

	std::string RemoteContainer::GetHandle()
	{
		return handle_;
	}

}
