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

	using std::shared_ptr;
	using boost::scoped_ptr;
	using boost::system::error_code;

	using boost::lexical_cast;
	using boost::bad_lexical_cast;

	using boost::split;
	using boost::is_any_of;

	using std::tuple;

	using std::make_pair;
	using std::string;
	using std::stringstream;
	using std::istream;

	namespace asio = boost::asio;
	using namespace boost::asio::ip;

	//
	// I've found those numbers testing with a group containing 50k containers
	//
#ifdef _DEBUG
	int TioTcpSession::PENDING_SEND_SIZE_BIG_THRESHOLD = 1024 * 1024;
	int TioTcpSession::PENDING_SEND_SIZE_SMALL_THRESHOLD = 1024;
#else
	int TioTcpSession::PENDING_SEND_SIZE_BIG_THRESHOLD = 1024 * 1024 * 10;
	int TioTcpSession::PENDING_SEND_SIZE_SMALL_THRESHOLD = 1024;
#endif

	std::ostream& TioTcpSession::logstream_ = std::cout;
	
	TioTcpSession::TioTcpSession(asio::io_service& io_service, TioTcpServer& server, unsigned int id) :
		io_service_(io_service),
		socket_(io_service),
		server_(server),
		lastHandle_(0),
		valid_(true),
        pendingSendSize_(0),
		maxPendingSendingSize_(0),
		sentBytes_(0),
		id_(id),
		binaryProtocol_(false)
	{
		return;
	}
	
	TioTcpSession::~TioTcpSession()
	{
		BOOST_ASSERT(subscriptions_.empty());
		BOOST_ASSERT(diffs_.empty());
		BOOST_ASSERT(handles_.empty());
		BOOST_ASSERT(poppers_.empty());

		logstream_ << "session " << id_ << " just died" << endl;

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

		unsigned int cookie = container->Subscribe(
			[container, destinationContainer](const string& event_name, const TioData& key, const TioData& value, const TioData& metadata)
			{
				MapContainerMirror(container, destinationContainer, event_name, key, value, metadata);
			},
			"__none__"); // "__none__" will make us receive only the updates (not the snapshot)

		diffs_[handle] = make_pair(destinationContainer, cookie); 
	}

	unsigned int TioTcpSession::id()
	{
		return id_;

	}

	bool TioTcpSession::UsesBinaryProtocol() const
	{
		return binaryProtocol_;
	}

	tcp::socket& TioTcpSession::GetSocket()
	{
		return socket_;
	}

	void TioTcpSession::OnAccept()
	{
		logstream_ << "new connection, id=" << id_ << std::endl;

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
			SendTextEvent(handle, key, value, metadata, eventName);
	}

	
	void TioTcpSession::BinaryWaitAndPopNext(unsigned int handle)
	{
		shared_ptr<ITioContainer> container = GetRegisteredContainer(handle);

		//
		// already subscribed
		//
		if(poppers_.find(handle) != poppers_.end())
			throw std::runtime_error(string("wait and pop next command already pending for handle ") + lexical_cast<string>(handle));

		unsigned int popId;

		auto shared_this = shared_from_this();
		
		popId = container->WaitAndPopNext(
			[shared_this, handle](const string& eventName, const TioData& key, const TioData& value, const TioData& metadata)
			{
				shared_this->OnPopEvent(handle, eventName, key, value, metadata);
			});

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

		auto shared_this = shared_from_this();

		asio::async_read(
					socket_, 
					asio::buffer(buffer, header->message_size),
					[shared_this, message](const error_code& err, size_t read)
					{
						shared_this->OnBinaryProtocolMessage(message, err);
						
					});
	}

	void TioTcpSession::ReadBinaryProtocolMessage()
	{
		shared_ptr<PR1_MESSAGE_HEADER> header(new PR1_MESSAGE_HEADER);

		auto shared_this = shared_from_this();

		asio::async_read(
					socket_, 
					asio::buffer(header.get(), sizeof(PR1_MESSAGE_HEADER)),
					[shared_this, header](const error_code& err, size_t read)
					{
						shared_this->OnBinaryProtocolMessageHeader(header, err);
					});
	}

	void TioTcpSession::ReadCommand()
	{
		currentCommand_ = Command();

		auto shared_this = shared_from_this();

		asio::async_read_until(socket_, buf_, '\n', 
			[shared_this](const error_code& err, size_t read)
			{
				shared_this->OnReadCommand(err, read);
			});
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
			BOOST_ASSERT(moreDataSize < 256 * 1024 * 1024);

			if(buf_.size() >= moreDataSize)
			{
				auto shared_this = shared_from_this();

				io_service_.post(
					[shared_this, moreDataSize]()
					{
						shared_this->OnCommandData(moreDataSize, boost::system::error_code(), moreDataSize);
					});
			}
			else
			{
				auto shared_this = shared_from_this();

				asio::async_read(
					socket_, buf_, asio::transfer_at_least(moreDataSize - buf_.size()),
					[shared_this, moreDataSize](const error_code& err, size_t read)
					{
						shared_this->OnCommandData(moreDataSize, err, read);
					});
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

	void TioTcpSession::SendTextEvent(unsigned int handle, const TioData& key, const TioData& value, const TioData& metadata, const string& eventName )
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

	

	bool TioTcpSession::ShouldSendEvent(const shared_ptr<SUBSCRIPTION_INFO>& subscriptionInfo, string eventName, 
		const TioData& key, const TioData& value, const TioData& metadata, std::vector<EXTRA_EVENT>* extraEvents)
	{
		if(subscriptionInfo->eventFilterStart == 0 && subscriptionInfo->eventFilterEnd == -1)
			return true;

		int currentIndex = 0;
		int recordCount = subscriptionInfo->container->GetRecordCount();

		if(eventName == "pop_front")
		{
			currentIndex = 0;
			eventName = "delete";
		}
		else if(eventName == "pop_back")
		{
			currentIndex = recordCount - 1;
			eventName = "delete";
		}
		else if(eventName == "push_front")
		{
			currentIndex = 0;
			eventName = "insert";
		}
		else
		{
			if(key.GetDataType() != TioData::Int)
				return true;

			currentIndex = key.AsInt();
		}

		
		int realFilterStart = NormalizeIndex(subscriptionInfo->eventFilterStart,
			recordCount,
			false);

		int realFilterEnd = NormalizeIndex(subscriptionInfo->eventFilterEnd,
			recordCount,
			false);
		//
		//
		// When generating events, we must always grow the slice beyond it's size instead
		// of shrinking it when necessary. 
		// Example: if the slice is [0:9] and client deletes a record inside
		// the range, we must send the push_back event (to add a last record to the slice)
		// before sending the delete. So, after the push_back event the slice will have
		// 11 items. But it's better than having a shorter slice. It would happen if we sent
		// the delete event before the push_back
		//
		// So, events that add records to the list will be sent by this function. Events
		// that will shrink the list will be added to extraEvents, to be send after the
		// real event
		//

		//
		// IMPORTANT: the container was already changed. So, if we are handling a push_back,
		// the item is already inside the container. And the recordCount reflect this, of course
		//
		if(eventName == "push_back")
		{
			if(currentIndex >= realFilterStart && currentIndex <= realFilterEnd)
			{
				if(realFilterStart == 0)
					return true;
				else
				{
					//
					// On push_back, we must send key as the index of item, which
					// is the last one. On slice, we must fix the index (key)
					//
					SendEvent(subscriptionInfo, "push_back",
						currentIndex - realFilterStart, value, metadata);

					return false;
				}
			}	
			else
				return false;
		}
		else if(eventName == "delete")
		{
			bool shouldSendEvent = true;

			if(currentIndex > realFilterEnd)
				return false;
			
			//
			// If some record is deleted before the filter start (for example, filter starts
			// at 10 and record 5 was deleted) the records will be shifted. So we will delete
			// the first item to cause this effect
			//
			if(currentIndex <= realFilterStart)
			{
				extraEvents->push_back(
					EXTRA_EVENT(
						0, 
						subscriptionInfo->container, 
						"pop_front",
						false)
					);

				shouldSendEvent = false;
			}

			if(recordCount && recordCount > realFilterEnd)
			{
				EXTRA_EVENT pushBackEvent(
					realFilterEnd, 
					subscriptionInfo->container, 
					"push_back",
					true);

				//
				// We will send a push_back, but client still has the current deleted
				// record (we didn't send the delete event yet at this point). So we need
				// to adjust the push_back key accordingly
				//
				pushBackEvent.key.Set(realFilterEnd + 1 - realFilterStart);

				SendEvent(subscriptionInfo, "push_back", 
					pushBackEvent.key, pushBackEvent.value, pushBackEvent.metadata);
			}

			if(realFilterStart > 0 && shouldSendEvent)
			{
				//
				// adjust index to be slice related
				//
				SendEvent(subscriptionInfo, "delete",
					currentIndex - realFilterStart, TIONULL, TIONULL);

				return false;
			}

			return shouldSendEvent;
		}
		else if(eventName == "insert")
		{
			bool shouldSendEvent = true;

			//
			// If some record is inserted before the filter start the records will 
			// be shifted right. So the first record changed. We should add it
			//
			if(currentIndex < realFilterStart)
			{
				EXTRA_EVENT pushFrontEvent(
						realFilterStart, 
						subscriptionInfo->container, 
						"push_front",
						true);
				
				SendEvent(subscriptionInfo, "push_front", 
					0, pushFrontEvent.value, pushFrontEvent.metadata);

				shouldSendEvent = false;
			}

			//
			// The records were shifted. We should delete the last one
			//
			if(recordCount - 1 > realFilterEnd)
			{
				extraEvents->push_back(
					EXTRA_EVENT(
						realFilterEnd, 
						subscriptionInfo->container, 
						"pop_back",
						false)
					);
			}

			if(realFilterStart > 0 && shouldSendEvent)
			{
				//
				// adjust index to be slice related
				//
				SendEvent(subscriptionInfo, "insert",
					currentIndex - realFilterStart, value, metadata);

				return false;
			}

			return shouldSendEvent;
		}
		else if(eventName == "set")
		{
			if(currentIndex < realFilterStart || currentIndex > realFilterEnd)
				return false;
			
			SendEvent(subscriptionInfo, "set", currentIndex - realFilterStart, value, metadata);
			return false;
		}
		
		return true;
	}

	void TioTcpSession::SendEvent(shared_ptr<SUBSCRIPTION_INFO> subscriptionInfo, const string& eventName, 
		const TioData& key, const TioData& value, const TioData& metadata)
	{
		if(subscriptionInfo->binaryProtocol)
			SendBinaryEvent(subscriptionInfo->handle, key, value, metadata, eventName);
		else
			SendTextEvent(subscriptionInfo->handle, key, value, metadata, eventName);
	}


	void TioTcpSession::OnEvent(shared_ptr<SUBSCRIPTION_INFO> subscriptionInfo, const string& eventName, 
		const TioData& key, const TioData& value, const TioData& metadata)
	{
		if(!valid_)
			return;

		vector<EXTRA_EVENT> extraEvents;
		
		bool shouldSend = ShouldSendEvent(subscriptionInfo, eventName, key, value, metadata, &extraEvents);

		if(shouldSend)
			SendEvent(subscriptionInfo, eventName, key, value, metadata);

		for(vector<EXTRA_EVENT>::const_iterator i = extraEvents.begin() ; i != extraEvents.end() ; ++i)
		{
			const EXTRA_EVENT& extraEvent = *i;

			SendEvent(subscriptionInfo, 
				extraEvent.eventName,
				extraEvent.key,
				extraEvent.value,
				extraEvent.metadata);
		}
	}

	void TioTcpSession::SendResultSetStart(unsigned int queryID)
	{
		stringstream answer;
		answer << "answer ok query " << queryID << "\r\n";

		SendAnswer(answer);
	}

	void TioTcpSession::SendResultSetEnd(unsigned int queryID)
	{
		stringstream answer;
		answer << "query " << queryID << " end\r\n";
		SendAnswer(answer);
	}

	void TioTcpSession::SendResultSet(shared_ptr<ITioResultSet> resultSet, unsigned int queryID)
	{
		SendResultSetStart(queryID);

		for(;;)
		{
			TioData key, value, metadata;
			
			bool b = resultSet->GetRecord(&key, &value, &metadata);

			if(!b)
			{
				SendResultSetEnd(queryID);
				break;
			}

			SendResultSetItem(queryID, key, value, metadata);

			resultSet->MoveNext();
		}
	}

	void TioTcpSession::SendBinaryResultSet(shared_ptr<ITioResultSet> resultSet, unsigned int queryID, 
		function<bool(const TioData& key)> filterFunction, unsigned maxRecords)
	{
		shared_ptr<PR1_MESSAGE> answer = Pr1CreateAnswerMessage();
		
		pr1_message_add_field_int(answer.get(), MESSAGE_FIELD_ID_QUERY_ID, queryID);

		SendBinaryMessage(answer);

		if(maxRecords == 0)
			maxRecords = 0xFFFFFFFF;
		
		for(unsigned a = 0; a < maxRecords; )
		{
			TioData key, value, metadata;

			bool b = resultSet->GetRecord(&key, &value, &metadata);

			if(!b)
				break;

			resultSet->MoveNext();

			if(filterFunction && !filterFunction(key))
				continue;

			shared_ptr<PR1_MESSAGE> item = Pr1CreateMessage();

			Pr1MessageAddField(item.get(), MESSAGE_FIELD_ID_COMMAND, TIO_COMMAND_QUERY_ITEM);
			Pr1MessageAddField(item.get(), MESSAGE_FIELD_ID_QUERY_ID, queryID);
			Pr1MessageAddFields(item, &key, &value, &metadata);
			SendBinaryMessage(item);

			++a;
		
			
		}

		//
		// no more records, we'll just send an empty publication
		//
		shared_ptr<PR1_MESSAGE> queryEnd = Pr1CreateMessage();

		Pr1MessageAddField(queryEnd.get(), MESSAGE_FIELD_ID_COMMAND, TIO_COMMAND_QUERY_ITEM);
		Pr1MessageAddField(queryEnd.get(), MESSAGE_FIELD_ID_QUERY_ID, queryID);
		SendBinaryMessage(queryEnd);
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
		if(!valid_)
			return;

        if(pendingSendSize_)
        {
			//
			// If there is too much data pending, the client is not 
			// receiving it anymore. We're going to disconnect him, otherwise
			// we will consume too much memory
			//
			if(pendingSendSize_ > 100 * 1024 * 1024)
			{
				UnsubscribeAll();
				server_.OnClientFailed(shared_from_this(), boost::system::error_code());
				return;
			}

            pendingSendData_.push(str);
            return;
        }
        else
            SendStringNow(str);

    }

	void TioTcpSession::SendStringNow(const string& str)
	{
		if(!valid_)
			return;

		size_t answerSize = str.size();

		char* buffer = new char[answerSize];
		memcpy(buffer, str.c_str(), answerSize);

		IncreasePendingSendSize(answerSize);

		auto shared_this = shared_from_this();

		asio::async_write(
			socket_,
			asio::buffer(buffer, answerSize), 
			[shared_this, buffer, answerSize](const error_code& err, size_t sent)
			{
				shared_this->OnWrite(buffer, answerSize, err, sent);
			});
	}

	void TioTcpSession::OnWrite(char* buffer, size_t bufferSize, const error_code& err, size_t sent)
	{
		delete[] buffer;

        pendingSendSize_ -= bufferSize;

		sentBytes_ += sent;

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

	void TioTcpSession::InvalidateConnection(const error_code& err)
	{
		if(!IsValid())
			return;

		UnsubscribeAll();

		server_.OnClientFailed(shared_from_this(), err);

		socket_.close();

		valid_ = false;
	}

	bool TioTcpSession::IsValid()
	{
		return valid_;
	}

	bool TioTcpSession::CheckError(const error_code& err)
	{
		if(!!err)
		{
			//
			// We can get here several times for the same connection if we have lots of pending writes
			//
			if(IsValid())
			{
				//logstream_ << "error on connection " << id_ << ": " << err.message() << endl;
			}

			InvalidateConnection(err);
			
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

		for(WaitAndPopNextMap::const_iterator i = poppers_.begin() ;  i != poppers_.end() ; ++i)
		{
			int handle, popId;
			pair_assign(handle, popId) = *i;

			GetRegisteredContainer(handle)->CancelWaitAndPopNext(popId);
		}

		poppers_.clear();

		StopDiffs();

		handles_.clear();
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

	void TioTcpSession::Subscribe(unsigned int handle, const string& start, int filterEnd, bool sendAnswer)
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

			//
			// This will make Tio not to lock until it finished sending 
			// snapshot to a client. But it makes the logic more complicated.
			// Now all the events are being buffered at session level if it
			// fill up the connection buffer, so it's not a huge problem
			// and the lock up time will not be that big, unless the container
			// is really big
			//
#if 0

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

			if(sendAnswer)
				SendString("answer ok\r\n");
			
			SendPendingSnapshots();

			return;
#endif // #if 0

		}
		catch(boost::bad_lexical_cast&)
		{

		}

		int numericStart = 0;
		
		boost::conversion::try_lexical_convert<int>(start, numericStart);

		subscriptionInfo->eventFilterStart = numericStart;
		subscriptionInfo->eventFilterEnd = filterEnd;

		subscriptions_[handle] = subscriptionInfo;

		try
		{
			auto shared_this = shared_from_this();

			subscriptionInfo->cookie = container->Subscribe(
				[shared_this, subscriptionInfo](const string& eventName, const TioData& key, const TioData& value, const TioData& metadata)
				{
					shared_this->OnEvent(subscriptionInfo, eventName, key, value, metadata);

				}, start);
			
			if(sendAnswer)
				SendString("answer ok\r\n");
		}
		catch(std::exception& ex)
		{
			subscriptions_.erase(handle);
			SendString(string("answer error ") + ex.what() + "\r\n");
		}

		return;
	}


	void TioTcpSession::BinarySubscribe(unsigned int handle, const string& start, bool sendAnswer)
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

		//
		// We are not checking if the container is changing during the snapshot,
		// it can cause problems and crashes. 
		//
#if 0
		if(!start.empty())
		{
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
		}
#endif

		//
		// if we're here, start is not numeric. We'll let the container deal with this
		//
		subscriptions_[handle] = subscriptionInfo;

		try
		{
			//
			// TODO: this isn't really right, I'm just doing this to send the
			// answer before the events. Is the the subscription fails, we're screwed,
			// since we're sending a success answer before doing the subscription
			//
			if(sendAnswer)
				SendBinaryAnswer();

			auto shared_this = shared_from_this();

			subscriptionInfo->cookie = container->Subscribe(
				[shared_this, subscriptionInfo](const string& eventName, const TioData& key, const TioData& value, const TioData& metadata)
				{
					shared_this->OnEvent(subscriptionInfo, eventName, key, value, metadata);
				}, 
				start);
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
		// This feature is disabled and we should never get here
		//
		BOOST_ASSERT(false);

		//
		// TODO: hard coded counter
		//
		for(unsigned int a = 0 ; a < 10 * 1000 ; a++)
		{
			if(pendingSnapshots_.empty())
				return;

			std::list<unsigned int> toRemove;

			BOOST_FOREACH(SubscriptionMap::value_type& p, pendingSnapshots_)
			{
				unsigned int handle;
				string event_name;
				shared_ptr<SUBSCRIPTION_INFO> subscriptionInfo;
				TioData searchKey, key, value, metadata;

				pair_assign(handle, subscriptionInfo) = p;

				if(subscriptionInfo->resultSet)
				{
					bool b;
					
					b = subscriptionInfo->resultSet->GetRecord(&key, &value, &metadata);

					if(b)
					{
						OnEvent(subscriptionInfo, subscriptionInfo->event_name, key, value, metadata);
						subscriptionInfo->nextRecord++;
					}

					if(!b || !subscriptionInfo->resultSet->MoveNext())
					{
						// done
						auto shared_this = shared_from_this();

						subscriptionInfo->cookie = subscriptionInfo->container->Subscribe(
							[shared_this, subscriptionInfo](const string& eventName, const TioData& key, const TioData& value, const TioData& metadata)
							{
								shared_this->OnEvent(subscriptionInfo, eventName, key, value, metadata);
							}, "");

						toRemove.push_back(handle);

						continue;
					}
				}
				else
				{
					unsigned int recordCount = subscriptionInfo->container->GetRecordCount();

					if(recordCount)
					{
						searchKey = static_cast<int>(subscriptionInfo->nextRecord);

						subscriptionInfo->container->GetRecord(searchKey, &key, &value, &metadata);

						OnEvent(subscriptionInfo, subscriptionInfo->event_name, key, value, metadata);

						subscriptionInfo->nextRecord++;
					}

					if(recordCount == 0 || subscriptionInfo->nextRecord >= recordCount)
					{
						// done
						auto shared_this = shared_from_this();

						subscriptionInfo->cookie = subscriptionInfo->container->Subscribe(
							[shared_this, subscriptionInfo](const string& eventName, const TioData& key, const TioData& value, const TioData& metadata)
							{
								shared_this->OnEvent(subscriptionInfo, eventName, key, value, metadata);
							}
							, "");

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
		else if(eventName == "snapshot_end")
			return TIO_EVENT_SNAPSHOT_END;

		return 0;
	}

	int EventCodeToEventName(const string& eventName)
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
		else if(eventName == "snapshot_end")
			return TIO_EVENT_SNAPSHOT_END;

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

	void TioTcpSession::SendBinaryErrorAnswer(int errorCode, const string& description)
	{
		shared_ptr<PR1_MESSAGE> answer = Pr1CreateMessage();

		pr1_message_add_field_int(answer.get(), MESSAGE_FIELD_ID_COMMAND, TIO_COMMAND_ANSWER);
		pr1_message_add_field_int(answer.get(), MESSAGE_FIELD_ID_ERROR_CODE, errorCode);
		pr1_message_add_field_string(answer.get(), MESSAGE_FIELD_ID_ERROR_DESC, description.c_str());

#ifdef _DEBUG
		logstream_ << "ERROR: " << errorCode << ": " << description << endl;
#endif

		SendBinaryMessage(answer);
	}

	void TioTcpSession::SendPendingBinaryData()
	{
		if(!beingSendData_.empty())
			return;

		if(pendingBinarySendData_.empty())
			return;

		static const int SEND_BUFFER_SIZE = 10 * 1024 * 1024;

		if(!binarySendBuffer_)
			binarySendBuffer_.reset(new char[SEND_BUFFER_SIZE]);

		int bufferSpaceUsed = 0;
		char* nextBufferSpace = binarySendBuffer_.get();

		while(!pendingBinarySendData_.empty())
		{
			const shared_ptr<PR1_MESSAGE>& item = pendingBinarySendData_.front();

			void* buffer;
			unsigned int bufferSize;

			pr1_message_get_buffer(item.get(), &buffer, &bufferSize);

			if(bufferSpaceUsed + bufferSize > SEND_BUFFER_SIZE)
				break;

			memcpy(nextBufferSpace, buffer, bufferSize);
			nextBufferSpace += bufferSize;
			bufferSpaceUsed += bufferSize;

			pendingBinarySendData_.pop_front();
		}

		auto shared_this = shared_from_this();

		asio::async_write(
			socket_,
			asio::buffer(binarySendBuffer_.get(), bufferSpaceUsed),
			[shared_this](const error_code& err, size_t sent)
		{
			shared_this->OnBinaryMessageSent(err, sent);
		});
	}

	void TioTcpSession::OnBinaryMessageSent(const error_code& err, size_t sent)
	{
		if(CheckError(err))
		{
			//std::cerr << "ERROR sending binary data: " << err << std::endl;
			return;
		}


		DecreasePendingSendSize(sent);
		sentBytes_ += sent;

		BOOST_ASSERT(pendingSendSize_ >= 0);

		SendPendingSnapshots();

		SendPendingBinaryData();
	}

	void TioTcpSession::RegisterLowPendingBytesCallback(std::function<void(shared_ptr<TioTcpSession>)> lowPendingBytesThresholdCallback)
	{
		BOOST_ASSERT(IsPendingSendSizeTooBig());
		lowPendingBytesThresholdCallbacks_.push(lowPendingBytesThresholdCallback);
		logstream_ << "RegisterLowPendingBytesCallback, " << lowPendingBytesThresholdCallbacks_.size() << " callbacks" << endl;
	}

	void TioTcpSession::DecreasePendingSendSize(int size)
	{
		pendingSendSize_ -= size;

		BOOST_ASSERT(pendingSendSize_ >= 0);

		if(pendingSendSize_ <= PENDING_SEND_SIZE_SMALL_THRESHOLD && !lowPendingBytesThresholdCallbacks_.empty())
		{
			logstream_ << "lowPendingBytesThresholdCallbacks_, id= " << id_ << ", "
				<< lowPendingBytesThresholdCallbacks_.size() << " still pending" << endl;

			// local copy, so callback can register another callback
			auto callback = lowPendingBytesThresholdCallbacks_.front();
			lowPendingBytesThresholdCallbacks_.pop();

			auto shared_this = shared_from_this();
			server_.PostCallback([shared_this, callback]{callback(shared_this); });
		}
	}

	void TioTcpSession::SendBinaryMessage(const shared_ptr<PR1_MESSAGE>& message)
	{
		if(!valid_)
			return;

		pendingBinarySendData_.push_back(message);

		IncreasePendingSendSize(pr1_message_get_data_size(message.get()));

		SendPendingBinaryData();
	}

	void TioTcpSession::SendBinaryAnswer(TioData* key, TioData* value, TioData* metadata)
	{
		SendBinaryMessage(Pr1CreateAnswerMessage(key, value, metadata));
	}

	void TioTcpSession::SendBinaryAnswer()
	{
		SendBinaryMessage(Pr1CreateAnswerMessage(NULL, NULL, NULL));
	}

} // namespace tio


