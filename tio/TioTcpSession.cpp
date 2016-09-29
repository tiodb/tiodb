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

#include "pch.h"
#include "TioTcpSession.h"
#include "TioTcpServer.h"

namespace tio
{
	
	using std::cout;
	using std::cerr;
	using std::endl;

	using boost::shared_ptr;
	using boost::scoped_ptr;
	using boost::system::error_code;

	using boost::lexical_cast;
	using boost::bad_lexical_cast;

	using boost::split;
	using boost::is_any_of;

	using boost::tuple;

	using std::make_pair;
	using std::string;
	using std::stringstream;
	using std::istream;

	namespace asio = boost::asio;
	using namespace boost::asio::ip;

	
	TioTcpSession::TioTcpSession(asio::io_service& io_service, TioTcpServer& server, unsigned int id) :
		io_service_(io_service),
		socket_(io_service),
		server_(server),
		lastHandle_(0),
        pendingSendSize_(0),
		id_(id),
		binaryProtocol_(false)
	{
		return;
	}
	
	TioTcpSession::~TioTcpSession()
	{
		BOOST_ASSERT(subscriptions_.empty());
		BOOST_ASSERT(diffs_.empty());

		return;
	}


	void TioTcpSession::StopDiffs()
	{
		for(DiffMap::iterator i = diffs_.begin() ; i != diffs_.end() ; ++i)
		{
			i->second.first->Unsubscribe(i->second.second);

			//
			// TODO: clear diff contents
			//
		}

		diffs_.clear();
	}

	shared_ptr<ITioContainer> TioTcpSession::GetDiffDestinationContainer(unsigned int handle)
	{
		DiffMap::const_iterator i = diffs_.find(handle);

		if(i != diffs_.end())
			return i->second.first;
		else
			return shared_ptr<ITioContainer>();
	}

	void MapContainerMirror(shared_ptr<ITioContainer> source, shared_ptr<ITioContainer> destination,
		const string& event_name, const TioData& key, const TioData& value, const TioData& metadata)
	{
		if(event_name == "set" || event_name == "insert")
			destination->Set(key, value, event_name);
		if(event_name == "delete")
			destination->Delete(key, TIONULL, event_name);
		else if(event_name == "clear")
		{
			//
			// on a clear, we'll set an delete event for every source record
			//
			shared_ptr<ITioResultSet> query = source->Query(0,0, TIONULL);

			TioData key;

			while(query->GetRecord(&key, NULL, NULL))
			{
				destination->Set(key, TIONULL, "delete");
				query->MoveNext();
			}
		}

		//
		// TODO: support other events
		//
	}

	void TioTcpSession::SetupDiffContainer(unsigned int handle, 
		shared_ptr<ITioContainer> destinationContainer)
	{
		shared_ptr<ITioContainer> container = GetRegisteredContainer(handle);

		unsigned int cookie = container->Subscribe(boost::bind(&MapContainerMirror, container, destinationContainer, _1, _2, _3, _4), 
			"__none__"); // "__none__" will make us receive only the updates (not the snapshot)

		diffs_[handle] = make_pair(destinationContainer, cookie); 
	}

	unsigned int TioTcpSession::GetID()
	{
		return id_;

	}

	tcp::socket& TioTcpSession::GetSocket()
	{
		return socket_;
	}

	void TioTcpSession::OnAccept()
	{
		#ifdef _DEBUG
		std::cout << "<< new connection" << std::endl;
		#endif

		socket_.set_option(tcp::no_delay(true));
		ReadCommand();
	}

	
	void TioTcpSession::OnBinaryProtocolMessage(PR1_MESSAGE* message, const error_code& err)
	{
		// this thing will delete the pointer
		shared_ptr<PR1_MESSAGE> messageHolder(message, &pr1_message_delete);

		if(CheckError(err))
			return;

		server_.OnBinaryCommand(shared_from_this(), message);

		ReadBinaryProtocolMessage();
	}

	void TioTcpSession::OnPopEvent(unsigned int handle, const string& eventName, const TioData& key, const TioData& value, const TioData& metadata)
	{
		WaitAndPopNextMap::iterator i = poppers_.find(handle);
		
		if(i != poppers_.end())
			poppers_.erase(i);

		if(binaryProtocol_)
			SendBinaryEvent(handle, key, value, metadata, eventName);
		else
			SendEvent(handle, key, value, metadata, eventName);
	}

	
	void TioTcpSession::BinaryWaitAndPopNext(unsigned int handle)
	{
		shared_ptr<ITioContainer> container = GetRegisteredContainer(handle);

		//
		// already subscribed
		//
		if(poppers_.find(handle) != poppers_.end())
			throw std::runtime_error("already subscribed");

		unsigned int popId;
		
		popId = container->WaitAndPopNext(boost::bind(&TioTcpSession::OnPopEvent, shared_from_this(), handle, _1, _2, _3, _4));

		//
		// id is zero id the pop is not pending
		//
		if(popId)
			poppers_[handle] = popId;

		return;

	}

	void TioTcpSession::OnBinaryProtocolMessageHeader(shared_ptr<PR1_MESSAGE_HEADER> header, const error_code& err)
	{
		if(CheckError(err))
			return;

		void* buffer;
		PR1_MESSAGE* message;

		message = pr1_message_new_get_buffer_for_receive(header.get(), &buffer);

		asio::async_read(
					socket_, 
					asio::buffer(buffer, header->message_size),
					boost::bind(
						&TioTcpSession::OnBinaryProtocolMessage, 
						shared_from_this(), 
						message,
						asio::placeholders::error)
						);
	}

	void TioTcpSession::ReadBinaryProtocolMessage()
	{
		shared_ptr<PR1_MESSAGE_HEADER> header(new PR1_MESSAGE_HEADER);

		asio::async_read(
					socket_, 
					asio::buffer(header.get(), sizeof(PR1_MESSAGE_HEADER)),
					boost::bind(
						&TioTcpSession::OnBinaryProtocolMessageHeader, 
						shared_from_this(), 
						header,
						asio::placeholders::error)
						);
	}

	void TioTcpSession::ReadCommand()
	{
		currentCommand_ = Command();

		asio::async_read_until(socket_, buf_, '\n', 
			boost::bind(&TioTcpSession::OnReadCommand, shared_from_this(), asio::placeholders::error, asio::placeholders::bytes_transferred));
	}

	void TioTcpSession::OnReadCommand(const error_code& err, size_t read)
	{
		if(CheckError(err))
			return;

		string str;
		stringstream answer;
		bool moreDataToRead = false;
		size_t moreDataSize = 0;
		istream stream (&buf_);

		getline(stream, str);

		//
		// can happen if client send binary data
		//
		if(str.empty())
		{
			ReadCommand();
			return;
		}

		//
		// delete last \r if any
		//
		if(*(str.end() - 1) == '\r')
			str.erase(str.end() - 1);

		BOOST_ASSERT(currentCommand_.GetCommand().empty());
		
		currentCommand_.Parse(str.c_str());

		//
		// Check for protocol change. 
		//
		if(currentCommand_.GetCommand() == "protocol")
		{
			const Command::Parameters& parameters = currentCommand_.GetParameters();

			if(parameters.size() == 1 && parameters[0] == "binary")
			{
				SendAnswer("going binary");
				binaryProtocol_ = true;
				ReadBinaryProtocolMessage();
				return;
			}
		}

#ifdef _TIO_DEBUG
		cout << "<< " << str << endl;
#endif

		server_.OnCommand(currentCommand_, answer, &moreDataSize, shared_from_this());
		
		if(moreDataSize)
		{
			BOOST_ASSERT(moreDataSize < 1024 * 1024);

			if(buf_.size() >= moreDataSize)
			{
				io_service_.post(
					boost::bind(&TioTcpSession::OnCommandData, shared_from_this(), 
					moreDataSize, boost::system::error_code(), moreDataSize));
			}
			else
			{
				asio::async_read(
					socket_, buf_, asio::transfer_at_least(moreDataSize - buf_.size()),
					boost::bind(&TioTcpSession::OnCommandData, shared_from_this(), 
					moreDataSize, asio::placeholders::error, asio::placeholders::bytes_transferred));
			}

			moreDataToRead = true;
		}
	
		if(!answer.str().empty())
		{
			#ifdef _TIO_DEBUG
			string xx;
			getline(answer, xx);
			answer.seekp(0);
			cout << ">> " << xx << endl;
			#endif

			SendAnswer(answer);
		}

		if(!moreDataToRead)
			ReadCommand();
		
	}

	void TioTcpSession::OnCommandData(size_t dataSize, const error_code& err, size_t read)
	{
		if(CheckError(err))
			return;
		
		BOOST_ASSERT(buf_.size() >= dataSize);

		stringstream answer;
		size_t moreDataSize = 0;

		//
		// TODO: avoid this copy
		//
		shared_ptr<tio::Buffer>& dataBuffer = currentCommand_.GetDataBuffer();
		dataBuffer->EnsureMinSize(dataSize);

		buf_.sgetn((char*)dataBuffer->GetRawBuffer(), static_cast<std::streamsize>(dataSize));

		server_.OnCommand(currentCommand_, answer, &moreDataSize, shared_from_this());

		BOOST_ASSERT(moreDataSize == 0);

		SendAnswer(answer);

		#ifdef _TIO_DEBUG
		string xx;
		getline(answer, xx);
		cout << ">> " << xx << endl;
		#endif

		ReadCommand();
	}


	void TioTcpSession::SendAnswer(stringstream& answer)
	{
		BOOST_ASSERT(answer.str().size() > 0);

		SendString(answer.str());
	}

	void TioTcpSession::SendAnswer(const string& answer)
	{
		SendString(answer);
	}


	string TioDataToString(const TioData& data)
	{
		if(!data)
			return string();

		stringstream stream;

		stream << data;

		return stream.str();
	}

	void TioTcpSession::SendEvent(unsigned int handle, const TioData& key, const TioData& value, const TioData& metadata, const string& eventName )
	{
		stringstream answer;

		string keyString, valueString, metadataString;

		if(key)
			keyString = TioDataToString(key);

		if(value)
			valueString = TioDataToString(value);

		if(metadata)
			metadataString = TioDataToString(metadata);

		answer << "event " << handle << " " << eventName;

		if(!keyString.empty())
			answer << " key " << GetDataTypeAsString(key) << " " << keyString.length();

		if(!valueString.empty())
			answer << " value " << GetDataTypeAsString(value) << " " << valueString.length();

		if(!metadataString.empty())
			answer << " metadata " << GetDataTypeAsString(metadata) << " " << metadataString.length();

		answer << "\r\n";

		if(!keyString.empty())
			answer << keyString << "\r\n";

		if(!valueString.empty())
			answer << valueString << "\r\n";

		if(!metadataString.empty())
			answer << metadataString << "\r\n";

		SendString(answer.str());
	}


	void TioTcpSession::OnEvent(shared_ptr<SUBSCRIPTION_INFO> subscriptionInfo, const string& eventName, 
		const TioData& key, const TioData& value, const TioData& metadata)
	{
		if(subscriptionInfo->binaryProtocol)
			SendBinaryEvent(subscriptionInfo->handle, key, value, metadata, eventName);
		else
			SendEvent(subscriptionInfo->handle, key, value, metadata, eventName);

	}

	void TioTcpSession::SendResultSet(shared_ptr<ITioResultSet> resultSet, unsigned int queryID)
	{
		stringstream answer;
		answer << "answer ok query " << queryID << "\r\n";

		SendAnswer(answer);

		for(;;)
		{
			TioData key, value, metadata;
			
			bool b = resultSet->GetRecord(&key, &value, &metadata);

			if(!b)
			{
				stringstream answer;
				answer << "query " << queryID << " end\r\n";
				SendAnswer(answer);
				break;
			}

			SendResultSetItem(queryID, key, value, metadata);

			resultSet->MoveNext();
		}
	}

	void TioTcpSession::SendBinaryResultSet(shared_ptr<ITioResultSet> resultSet, unsigned int queryID)
	{
		shared_ptr<PR1_MESSAGE> answer = Pr1CreateAnswerMessage();
		
		pr1_message_add_field_int(answer.get(), MESSAGE_FIELD_ID_QUERY_ID, queryID);

		SendBinaryMessage(answer);
		
		for(;;)
		{
			TioData key, value, metadata;
			shared_ptr<PR1_MESSAGE> item = Pr1CreateMessage();

			Pr1MessageAddField(item.get(), MESSAGE_FIELD_ID_COMMAND, TIO_COMMAND_QUERY_ITEM);
			Pr1MessageAddField(item.get(), MESSAGE_FIELD_ID_QUERY_ID, queryID);

			bool b = resultSet->GetRecord(&key, &value, &metadata);

			//
			// is no more records, we'll just send an empty publication
			//
			if(b)
				Pr1MessageAddFields(item, &key, &value, &metadata);

			SendBinaryMessage(item);

			if(!b)
				break;
				
			resultSet->MoveNext();
		}
	}

	void TioTcpSession::SendResultSetItem(unsigned int queryID, 
		const TioData& key, const TioData& value, const TioData& metadata)
	{
		stringstream answer;

		/*
		if(itemType == "last");
		{
			answer << "query " << queryID << " end";
			SendString(answer.str());
			return;
		}
		*/

		string keyString, valueString, metadataString;

		if(key)
			keyString = TioDataToString(key);

		if(value)
			valueString = TioDataToString(value);

		if(metadata)
			metadataString = TioDataToString(metadata);

		answer << "query " << queryID << " item";

		if(!keyString.empty())
			answer << " key " << GetDataTypeAsString(key) << " " << keyString.length();

		if(!valueString.empty())
			answer << " value " << GetDataTypeAsString(value) << " " << valueString.length();

		if(!metadataString.empty())
			answer << " metadata " << GetDataTypeAsString(metadata) << " " << metadataString.length();

		answer << "\r\n";

		if(!keyString.empty())
			answer << keyString << "\r\n";

		if(!valueString.empty())
			answer << valueString << "\r\n";

		if(!metadataString.empty())
			answer << metadataString << "\r\n";

		SendString(answer.str());
	}

    void TioTcpSession::SendString(const string& str)
    {
        if(pendingSendSize_)
        {
            pendingSendData_.push(str);
            return;
        }
        else
            SendStringNow(str);

    }

	void TioTcpSession::SendStringNow(const string& str)
	{
		size_t answerSize = str.size();

		char* buffer = new char[answerSize];
		memcpy(buffer, str.c_str(), answerSize);

        pendingSendSize_ += answerSize;

		asio::async_write(
			socket_,
			asio::buffer(buffer, answerSize), 
			boost::bind(&TioTcpSession::OnWrite, shared_from_this(), buffer, answerSize, asio::placeholders::error, asio::placeholders::bytes_transferred));
	}

	void TioTcpSession::OnWrite(char* buffer, size_t bufferSize, const error_code& err, size_t read)
	{
		delete[] buffer;

        pendingSendSize_ -= bufferSize;

        if(CheckError(err))
		{
#ifdef _DEBUG
			cerr << "TioTcpSession::OnWrite ERROR: " << err << endl;
#endif
            return;
		}

        if(!pendingSendData_.empty())
        {
            SendStringNow(pendingSendData_.front());
            pendingSendData_.pop();
			return;
        }

		SendPendingSnapshots();

		return;
	}

	bool TioTcpSession::CheckError(const error_code& err)
	{
		if(!!err)
		{
			UnsubscribeAll();

			server_.OnClientFailed(shared_from_this(), err);

			return true;
		}

		return false;
	}

	void TioTcpSession::UnsubscribeAll()
	{
		pendingSnapshots_.clear();

		for(SubscriptionMap::iterator i = subscriptions_.begin() ; i != subscriptions_.end() ; ++i)
		{
			unsigned int handle;
			shared_ptr<SUBSCRIPTION_INFO> info;

			pair_assign(handle, info) = *i;

			GetRegisteredContainer(handle)->Unsubscribe(info->cookie);
		}

		subscriptions_.clear();

		StopDiffs();
	}


	unsigned int TioTcpSession::RegisterContainer(const string& containerName, shared_ptr<ITioContainer> container)
	{
		unsigned int handle = ++lastHandle_;
		handles_[handle] = make_pair(container, containerName);
		return handle;
	}

	shared_ptr<ITioContainer> TioTcpSession::GetRegisteredContainer(unsigned int handle, string* containerName, string* containerType)
	{
		HandleMap::iterator i = handles_.find(handle);

		if(i == handles_.end())
			throw std::invalid_argument("invalid handle");

		if(containerName)
			*containerName = i->second.second;

		if(containerType)
			*containerType = i->second.first->GetType();

		return i->second.first;
	}

	void TioTcpSession::CloseContainerHandle(unsigned int handle)
	{
		HandleMap::iterator i = handles_.find(handle);

		if(i == handles_.end())
			throw std::invalid_argument("invalid handle");

		Unsubscribe(handle);

		handles_.erase(i);
	}

	void TioTcpSession::Subscribe(unsigned int handle, const string& start)
	{
		shared_ptr<ITioContainer> container = GetRegisteredContainer(handle);

		//
		// already subscribed
		//
		if(subscriptions_.find(handle) != subscriptions_.end())
		{
			SendString("answer error already subscribed\r\n");
			return;
		}

		shared_ptr<SUBSCRIPTION_INFO> subscriptionInfo(new SUBSCRIPTION_INFO(handle));
		subscriptionInfo->container = container;

		try
		{
			int numericStart = lexical_cast<int>(start);
			
			//
			// lets try a query. Navigating a query is faster than accessing records
			// using index. Imagine a linked list being accessed by index every time...
			//
			try
			{
				subscriptionInfo->resultSet = container->Query(numericStart, 0, TIONULL);
			}
			catch(std::exception&)
			{
				//
				// no result set, don't care. We'll carry on with the indexed access
				//
			}

			subscriptionInfo->nextRecord = numericStart;

			if(IsListContainer(container))
				subscriptionInfo->event_name = "push_back";
			else if(IsMapContainer(container))
				subscriptionInfo->event_name = "set";
			else
				throw std::runtime_error("INTERNAL ERROR: container not a list neither a map");

			pendingSnapshots_[handle] = subscriptionInfo;
			
			subscriptions_[handle] = subscriptionInfo;
			SendString("answer ok\r\n");
			
			SendPendingSnapshots();

			return;
		}
		catch(boost::bad_lexical_cast&)
		{

		}

		//
		// if we're here, start is not numeric. We'll let the container deal with this
		//
		subscriptions_[handle] = subscriptionInfo;

		try
		{
			subscriptionInfo->cookie = container->Subscribe(
				boost::bind(&TioTcpSession::OnEvent, shared_from_this(), subscriptionInfo, _1, _2, _3, _4), start);
			
			SendString("answer ok\r\n");
		}
		catch(std::exception& ex)
		{
			subscriptions_.erase(handle);
			SendString(string("answer error ") + ex.what() + "\r\n");
		}

		return;
	}


	void TioTcpSession::BinarySubscribe(unsigned int handle, const string& start)
	{
		shared_ptr<ITioContainer> container = GetRegisteredContainer(handle);

		//
		// already subscribed
		//
		if(subscriptions_.find(handle) != subscriptions_.end())
			throw std::runtime_error("already subscribed");

		shared_ptr<SUBSCRIPTION_INFO> subscriptionInfo(new SUBSCRIPTION_INFO(handle));
		subscriptionInfo->container = container;
		subscriptionInfo->binaryProtocol = true;

		try
		{
			int numericStart = lexical_cast<int>(start);
			
			//
			// lets try a query. Navigating a query is faster than accessing records
			// using index. Imagine a linked list being accessed by index every time...
			//
			try
			{
				subscriptionInfo->resultSet = container->Query(numericStart, 0, TIONULL);
			}
			catch(std::exception&)
			{
				//
				// no result set, don't care. We'll carry on with the indexed access
				//
			}

			subscriptionInfo->nextRecord = numericStart;

			if(IsListContainer(container))
				subscriptionInfo->event_name = "push_back";
			else if(IsMapContainer(container))
				subscriptionInfo->event_name = "set";
			else
				throw std::runtime_error("INTERNAL ERROR: container not a list neither a map");

			pendingSnapshots_[handle] = subscriptionInfo;
			
			subscriptions_[handle] = subscriptionInfo;
			
			SendBinaryAnswer();
			
			SendPendingSnapshots();

			return;
		}
		catch(boost::bad_lexical_cast&)
		{

		}

		//
		// if we're here, start is not numeric. We'll let the container deal with this
		//
		subscriptions_[handle] = subscriptionInfo;

		try
		{
			//
			// TODO: this isn't really right, I`m just doing this to send the
			// answer before the events. Is the the subscription fails, we're screwed,
			// since we're sending a success answer before doing the subscription
			//
			SendBinaryAnswer();

			subscriptionInfo->cookie = container->Subscribe(
				boost::bind(&TioTcpSession::OnEvent, shared_from_this(), subscriptionInfo, _1, _2, _3, _4), start);
		}
		catch(std::exception&)
		{
			subscriptions_.erase(handle);
			throw;
		}

		return;
	}

	void TioTcpSession::SendPendingSnapshots()
	{
		if(pendingSnapshots_.empty())
			return;

		//
		// TODO: hard coded counter
		//
		for(unsigned int a = 0 ; a < 10 ; a++)
		{
			if(pendingSnapshots_.empty())
				return;

			std::list<unsigned int> toRemove;

			BOOST_FOREACH(SubscriptionMap::value_type& p, pendingSnapshots_)
			{
				unsigned int handle;
				string event_name;
				shared_ptr<SUBSCRIPTION_INFO> info;
				TioData searchKey, key, value, metadata;

				pair_assign(handle, info) = p;

				if(info->resultSet)
				{
					bool b;
					
					b = info->resultSet->GetRecord(&key, &value, &metadata);

					if(b)
					{
						OnEvent(info, info->event_name, key, value, metadata);
						info->nextRecord++;
					}

					if(!b || !info->resultSet->MoveNext())
					{
						// done
						info->cookie = info->container->Subscribe(
							boost::bind(&TioTcpSession::OnEvent, shared_from_this(), info, _1, _2, _3, _4), "");

						toRemove.push_back(handle);

						continue;
					}
				}
				else
				{
					unsigned int recordCount = info->container->GetRecordCount();

					if(recordCount)
					{
						searchKey = static_cast<int>(info->nextRecord);

						info->container->GetRecord(searchKey, &key, &value, &metadata);

						OnEvent(info, info->event_name, key, value, metadata);

						info->nextRecord++;
					}

					if(recordCount == 0 || info->nextRecord >= recordCount)
					{
						// done
						info->cookie = info->container->Subscribe(
							boost::bind(&TioTcpSession::OnEvent, shared_from_this(), info, _1, _2, _3, _4), "");

						toRemove.push_back(handle);

						continue;
					}
				}

			}

			BOOST_FOREACH(unsigned int h, toRemove)
			{
				pendingSnapshots_.erase(h);
			}
		}
	}

	void TioTcpSession::Unsubscribe(unsigned int handle)
	{
		SubscriptionMap::iterator i = subscriptions_.find(handle);
		
		if(i == subscriptions_.end())
			return; //throw std::invalid_argument("not subscribed");

		shared_ptr<ITioContainer> container = GetRegisteredContainer(handle);
		
		container->Unsubscribe(i->second->cookie);

		pendingSnapshots_.erase(i->first);
		subscriptions_.erase(i);
	}

	const vector<string>& TioTcpSession::GetTokens()
	{
		return tokens_;

	}

	void TioTcpSession::AddToken(const string& token)
	{
		tokens_.push_back(token);
	}

	int EventNameToEventCode(const string& eventName)
	{
		if(eventName == "push_back")
			return TIO_COMMAND_PUSH_BACK;
		else if(eventName == "push_front")
			return TIO_COMMAND_PUSH_FRONT;
		else if(eventName == "pop_back" || eventName == "pop_front" || eventName == "delete")
			return TIO_COMMAND_DELETE;
		else if(eventName == "clear")
			return TIO_COMMAND_CLEAR;
		else if(eventName == "set")
			return TIO_COMMAND_SET;
		else if(eventName == "insert")
			return TIO_COMMAND_INSERT;
		else if(eventName == "wnp_next")
			return TIO_COMMAND_WAIT_AND_POP_NEXT;

		return 0;
	}

	void TioTcpSession::SendBinaryEvent(int handle, const TioData& key, const TioData& value, const TioData& metadata, const string& eventName)
	{
		shared_ptr<PR1_MESSAGE> message = Pr1CreateMessage();

		Pr1MessageAddField(message.get(), MESSAGE_FIELD_ID_COMMAND, TIO_COMMAND_EVENT);
		Pr1MessageAddField(message.get(), MESSAGE_FIELD_ID_HANDLE, handle);
		Pr1MessageAddField(message.get(), MESSAGE_FIELD_ID_EVENT, EventNameToEventCode(eventName));

		if(key) Pr1MessageAddField(message.get(), MESSAGE_FIELD_ID_KEY, key);
		if(value) Pr1MessageAddField(message.get(), MESSAGE_FIELD_ID_VALUE, value);
		if(metadata) Pr1MessageAddField(message.get(), MESSAGE_FIELD_ID_METADATA, metadata);

		SendBinaryMessage(message);
	}


	

} // namespace tio


