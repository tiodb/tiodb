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
	using std::make_shared;
	using boost::scoped_ptr;
	using boost::system::error_code;

	using boost::lexical_cast;
	using boost::bad_lexical_cast;

	using boost::split;
	using boost::is_any_of;

	using std::tuple;

	using std::to_string;

	using std::make_pair;
	using std::string;
	using std::stringstream;
	using std::istream;

	namespace asio = boost::asio;
	using namespace boost::asio::ip;

	int EventNameToEventCode(const string& eventName)
	{
		if (eventName == "push_back")
			return TIO_COMMAND_PUSH_BACK;
		else if (eventName == "push_front")
			return TIO_COMMAND_PUSH_FRONT;
		else if (eventName == "pop_back" || eventName == "pop_front" || eventName == "delete")
			return TIO_COMMAND_DELETE;
		else if (eventName == "clear")
			return TIO_COMMAND_CLEAR;
		else if (eventName == "set")
			return TIO_COMMAND_SET;
		else if (eventName == "insert")
			return TIO_COMMAND_INSERT;
		else if (eventName == "snapshot_end")
			return TIO_EVENT_SNAPSHOT_END;

		return 0;
	}

	string EventCodeToEventName(ContainerEventCode eventCode)
	{
		switch (eventCode)
		{
		case TIO_COMMAND_PUSH_BACK: return "push_back";
		case TIO_COMMAND_PUSH_FRONT: return "push_front";
		case TIO_COMMAND_SET: return "set";
		case TIO_COMMAND_DELETE: return "delete";
		case TIO_COMMAND_INSERT: return "insert";
		case TIO_COMMAND_CLEAR: return "clear";
		case TIO_EVENT_SNAPSHOT_END: return "snapshot_end";
		default:
			BOOST_ASSERT(false && "**INVALID EVENT**");
			return "INVALID_EVENT";
		}
		return 0;
	}

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
		strand_(io_service),
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
		BOOST_ASSERT(handles_.empty());

		//logstream_ << "session " << id_ << " just died" << endl;

		return;
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
		//logstream_ << "new connection, id=" << id_ << std::endl;

		socket_.set_option(tcp::no_delay(true));

		ReadCommand();
	}

	void TioTcpSession::ReadBinaryProtocolMessage()
	{
		shared_ptr<PR1_MESSAGE_HEADER> header(new PR1_MESSAGE_HEADER);

		asio::async_read(
			socket_,
			asio::buffer(header.get(), sizeof(PR1_MESSAGE_HEADER)),
			[shared_this = shared_from_this(), header](const error_code& err, size_t read)
			{
				shared_this->OnBinaryProtocolMessageHeader(header, err);
			}
		);
	}


	void TioTcpSession::OnBinaryProtocolMessageHeader(shared_ptr<PR1_MESSAGE_HEADER> header, const error_code& err)
	{
		if (CheckError(err))
			return;

		void* buffer;
		PR1_MESSAGE* message;

		message = pr1_message_new_get_buffer_for_receive(header.get(), &buffer);

		asio::async_read(
			socket_,
			asio::buffer(buffer, header->message_size),
			wrap_callback(
			[shared_this = shared_from_this(), message](const error_code& err, size_t read)
			{
				shared_this->OnBinaryProtocolMessage(message, err);
			})
		);
	}

	void TioTcpSession::OnBinaryProtocolMessage(PR1_MESSAGE* message, const error_code& err)
	{
		// this thing will delete the pointer
		shared_ptr<PR1_MESSAGE> messageHolder(message, &pr1_message_delete);

		if (CheckError(err))
			return;

		server_.OnBinaryCommand(shared_from_this(), message);

		ReadBinaryProtocolMessage();
	}

	
	void TioTcpSession::ReadHttpCommand(const shared_ptr<HttpParser> httpParser)
	{
		lock_guard_t lock(bigLock_);

		if (buf_.size())
		{
			OnReadHttpCommand(httpParser, error_code(), buf_.size());
		}
		else
		{
			asio::async_read(socket_, buf_,
				wrap_callback(
					[shared_this = shared_from_this(), httpParser](const error_code& err, size_t read)
			{
				shared_this->OnReadHttpCommand(httpParser, err, read);
			}));
		}
	}

	void TioTcpSession::OnReadHttpCommand(const shared_ptr<HttpParser>& httpParser, const error_code& err, size_t read)
	{
		lock_guard_t lock(bigLock_);

		if (CheckError(err))
			return;

		const char* readData = asio::buffer_cast<const char*>(buf_.data());

		bool haveAFullMessage = httpParser->FeedBytes(readData, buf_.size());

		buf_.consume(buf_.size());

		if (httpParser->error())
		{
			InvalidateConnection(error_code());
			return;
		}

		if (!haveAFullMessage)
		{
			ReadHttpCommand(httpParser);
			return;
		}

		server_.OnHttpCommand(
			httpParser->currentMessage(),
			shared_from_this());
	}


	void TioTcpSession::ReadCommand()
	{
		lock_guard_t lock(bigLock_);

		currentCommand_ = Command();

		auto shared_this = shared_from_this();

		asio::async_read_until(socket_, buf_, '\n',
			wrap_callback(
				[shared_this](const error_code& err, size_t read)
		{
			shared_this->OnReadCommand(err, read);
		}));
	}

	void TioTcpSession::SendHttpResponseAndClose(
		int statusCode,
		const string& statusMessage,
		const string& mimeType,
		const map<string, string>* responseHeaders,
		const string& body)
	{
		auto httpAnswer = make_shared<string>();
		httpAnswer->reserve(body.size() + 100);

		//
		// TODO: send headers from responseHeaders parameter
		//
		httpAnswer->append("HTTP/1.1 " + to_string(statusCode) + " " + statusMessage + "\r\n");
		httpAnswer->append("Server: tiodb\r\n");
		httpAnswer->append("Content-Type: " + mimeType + "\r\n");
		httpAnswer->append("Connection: close\r\n");
		httpAnswer->append("Content-Length: " + to_string(body.size()) + "\r\n");
		httpAnswer->append("\r\n");
		httpAnswer->append(body);

		asio::async_write(
			socket_,
			asio::buffer(*httpAnswer),
			wrap_callback(
				[shared_this = shared_from_this(), httpAnswer](const error_code& err, size_t sent)
		{
			shared_this->InvalidateConnection(err);
		}
		));
	}

	void TioTcpSession::OnReadCommand(const error_code& err, size_t read)
	{
		if (CheckError(err))
			return;

		lock_guard_t lock(bigLock_);

		string str;
		stringstream answer;
		bool moreDataToRead = false;
		size_t moreDataSize = 0;
		istream stream(&buf_);

		getline(stream, str);

		//
		// can happen if client send binary data
		//
		if (str.empty())
		{
			ReadCommand();
			return;
		}

		//
		// delete last \r if any
		//
		if (*(str.end() - 1) == '\r')
			str.erase(str.end() - 1);

		BOOST_ASSERT(currentCommand_.GetCommand().empty());

		currentCommand_.Parse(str.c_str());

		//
		// Check for protocol change. 
		//
		if (currentCommand_.GetCommand() == "protocol")
		{
			const Command::Parameters& parameters = currentCommand_.GetParameters();

			if (parameters.size() == 1 && parameters[0] == "binary")
			{
				SendAnswer("going binary");
				binaryProtocol_ = true;
				ReadBinaryProtocolMessage();
				return;
			}
		}
		else if (currentCommand_.IsHttp())
		{
			auto httpParser = make_shared<HttpParser>();
			httpParser->FeedBytes(str.c_str(), str.size());

			// we removed line ending before, will need to insert it back
			httpParser->FeedBytes("\r\n", 2);
			ReadHttpCommand(httpParser);
			return;
		}

#ifdef _TIO_DEBUG
		cout << "<< " << str << endl;
#endif

		server_.OnTextCommand(currentCommand_, answer, &moreDataSize, shared_from_this());

		if (moreDataSize)
		{
			BOOST_ASSERT(moreDataSize < 256 * 1024 * 1024);

			if (buf_.size() >= moreDataSize)
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
					wrap_callback(
						[shared_this, moreDataSize](const error_code& err, size_t read)
				{
					shared_this->OnCommandData(moreDataSize, err, read);
				}));
			}

			moreDataToRead = true;
		}

		if (!answer.str().empty())
		{
#ifdef _TIO_DEBUG
			string xx;
			getline(answer, xx);
			answer.seekp(0);
			cout << ">> " << xx << endl;
#endif

			SendAnswer(answer);
		}

		if (!moreDataToRead)
			ReadCommand();

	}

	void TioTcpSession::OnCommandData(size_t dataSize, const error_code& err, size_t read)
	{
		if (CheckError(err))
			return;

		lock_guard_t lock(bigLock_);

		BOOST_ASSERT(buf_.size() >= dataSize);

		stringstream answer;
		size_t moreDataSize = 0;

		//
		// TODO: avoid this copy
		//
		shared_ptr<tio::Buffer>& dataBuffer = currentCommand_.GetDataBuffer();
		dataBuffer->EnsureMinSize(dataSize);

		buf_.sgetn((char*)dataBuffer->GetRawBuffer(), static_cast<std::streamsize>(dataSize));

		server_.OnTextCommand(currentCommand_, answer, &moreDataSize, shared_from_this());

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
		if (!data)
			return string();

		stringstream stream;

		stream << data;

		return stream.str();
	}

	void TioTcpSession::SendTextEvent(unsigned int handle, ContainerEventCode eventCode, const TioData& key, const TioData& value, const TioData& metadata)
	{
		stringstream answer;

		string keyString, valueString, metadataString;

		if (key)
			keyString = TioDataToString(key);

		if (value)
			valueString = TioDataToString(value);

		if (metadata)
			metadataString = TioDataToString(metadata);

		answer << "event " << handle << " " << EventCodeToEventName(eventCode);

		if (!keyString.empty())
			answer << " key " << GetDataTypeAsString(key) << " " << keyString.length();

		if (!valueString.empty())
			answer << " value " << GetDataTypeAsString(value) << " " << valueString.length();

		if (!metadataString.empty())
			answer << " metadata " << GetDataTypeAsString(metadata) << " " << metadataString.length();

		answer << "\r\n";

		if (!keyString.empty())
			answer << keyString << "\r\n";

		if (!valueString.empty())
			answer << valueString << "\r\n";

		if (!metadataString.empty())
			answer << metadataString << "\r\n";

		SendString(answer.str());
	}


	void TioTcpSession::SendTextResultSetStart(unsigned int queryID)
	{
		stringstream answer;
		answer << "answer ok query " << queryID << "\r\n";

		SendAnswer(answer);
	}

	void TioTcpSession::SendTextResultSetEnd(unsigned int queryID)
	{
		stringstream answer;
		answer << "query " << queryID << " end\r\n";
		SendAnswer(answer);
	}

	void TioTcpSession::SendTextResultSet(shared_ptr<ITioResultSet> resultSet, unsigned int queryID)
	{
		SendTextResultSetStart(queryID);

		for(;;)
		{
			TioData key, value, metadata;
			
			bool b = resultSet->GetRecord(&key, &value, &metadata);

			if(!b)
			{
				SendTextResultSetEnd(queryID);
				break;
			}

			SendTextResultSetItem(queryID, key, value, metadata);

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

	void TioTcpSession::SendSubscriptionSnapshot(const shared_ptr<ITioResultSet>& resultSet, unsigned int handle, ContainerEventCode eventCode)
	{
		if (binaryProtocol_)
			SendBinarySubscriptionSnapshot(resultSet, handle, eventCode);
		else
			SendTextSubscriptionSnapshot(resultSet, handle, eventCode);
	}

	void TioTcpSession::SendTextSubscriptionSnapshot(const shared_ptr<ITioResultSet>& resultSet, unsigned int handle, ContainerEventCode eventCode)
	{
		for (;;)
		{
			TioData key, value, metadata;

			bool b = resultSet->GetRecord(&key, &value, &metadata);		

			if (!b)
				break;

			SendTextEvent(handle, eventCode, key, value, metadata);

			resultSet->MoveNext();
		}
	}

	void TioTcpSession::SendBinarySubscriptionSnapshot(const shared_ptr<ITioResultSet>& resultSet, unsigned int handle, ContainerEventCode eventCode)
	{
		vector<shared_ptr<PR1_MESSAGE>> messages(resultSet->RecordCount());

		for (unsigned a = 0; ; resultSet->MoveNext(), ++a)
		{
			TioData key, value, metadata;

			bool b = resultSet->GetRecord(&key, &value, &metadata);

			if (!b)
				break;

			shared_ptr<PR1_MESSAGE>& item = messages[a];
			item = Pr1CreateMessage();

			Pr1MessageAddField(item.get(), MESSAGE_FIELD_ID_COMMAND, TIO_COMMAND_EVENT);
			Pr1MessageAddField(item.get(), MESSAGE_FIELD_ID_HANDLE, handle);
			Pr1MessageAddField(item.get(), MESSAGE_FIELD_ID_EVENT_CODE, eventCode);

			Pr1MessageAddFields(item, &key, &value, &metadata);
		}

		SendBinaryMessages(messages);
	}


	void TioTcpSession::SendTextResultSetItem(unsigned int queryID, 
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
		lock_guard_t lock(bigLock_);

		if (!valid_)
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
		lock_guard_t lock(bigLock_);

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
			wrap_callback(
			[shared_this, buffer, answerSize](const error_code& err, size_t sent)
			{
				shared_this->OnWrite(buffer, answerSize, err, sent);
			}));
	}

	void TioTcpSession::OnWrite(char* buffer, size_t bufferSize, const error_code& err, size_t sent)
	{
		delete[] buffer;

		lock_guard_t lock(bigLock_);

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

		return;
	}

	bool TioTcpSession::IsValid()
	{
		return valid_;
	}

	bool TioTcpSession::CheckError(const error_code& err)
	{
		if(!!err)
		{
			InvalidateConnection(err);
			return true;
		}

		return false;
	}

	void TioTcpSession::InvalidateConnection(const error_code& err)
	{
		{
			lock_guard_t lock(bigLock_);

			if (!IsValid())
				return;

			valid_ = false;

			binarySendBuffer_.reset();
			pendingBinarySendData_.clear();
			decltype(pendingSendData_) whyDontQueueHaveAMethodNamedClear;
			
			pendingSendData_.swap(whyDontQueueHaveAMethodNamedClear);

			UnsubscribeAll();

			handles_.clear();
		}

		server_.OnClientFailed(shared_from_this(), err);

		socket_.close();
	}

	void TioTcpSession::UnsubscribeAll()
	{
		lock_guard_t lock(bigLock_);
	}

	unsigned int TioTcpSession::RegisterContainer(const string& containerName, shared_ptr<ITioContainer> container)
	{
		lock_guard_t lock(bigLock_);

		unsigned int handle = ++lastHandle_;
		handles_.emplace(make_pair(handle, HandleInfo(container)));
		return handle;
	}

	shared_ptr<ITioContainer> TioTcpSession::GetRegisteredContainer(unsigned int handle, string* containerName, string* containerType)
	{
		lock_guard_t lock(bigLock_);

		HandleMap::iterator i = handles_.find(handle);

		if(i == handles_.end())
			throw std::invalid_argument("invalid handle");

		HandleInfo& handleInfo = i->second;

		if (containerName)
			*containerName = handleInfo.container->GetName();

		if (containerType)
			*containerType = handleInfo.container->GetName();

		return handleInfo.container;
	}

	void TioTcpSession::CloseContainerHandle(unsigned int handle)
	{
		lock_guard_t lock(bigLock_);

		HandleMap::iterator i = handles_.find(handle);

		if(i == handles_.end())
			throw std::invalid_argument("invalid handle");

		Unsubscribe(handle);

		handles_.erase(i);
	}

	const vector<string> TioTcpSession::GetTokens()
	{
		lock_guard_t lock(bigLock_);
		return tokens_;
	}

	void TioTcpSession::AddToken(const string& token)
	{
		lock_guard_t lock(bigLock_);
		
		tokens_.push_back(token);
	}

	bool TioTcpSession::PublishEvent(
		unsigned handle,
		ContainerEventCode eventCode,
		const TioData& k, const TioData& v, const TioData& m)
	{
		if (binaryProtocol_)
			SendBinaryEvent(handle, eventCode, k, v, m);
		else
			SendTextEvent(handle, eventCode, k, v, m);

		return true;
	}

	void TioTcpSession::SendBinaryEvent(unsigned handle,
		ContainerEventCode eventCode,
		const TioData& k, const TioData& v, const TioData& m)
	{
		shared_ptr<PR1_MESSAGE> message = Pr1CreateMessage();

		Pr1MessageAddField(message.get(), MESSAGE_FIELD_ID_COMMAND, TIO_COMMAND_EVENT);
		Pr1MessageAddField(message.get(), MESSAGE_FIELD_ID_HANDLE, handle);
		Pr1MessageAddField(message.get(), MESSAGE_FIELD_ID_EVENT_CODE, eventCode);

		if(k) Pr1MessageAddField(message.get(), MESSAGE_FIELD_ID_KEY, k);
		if(v) Pr1MessageAddField(message.get(), MESSAGE_FIELD_ID_VALUE, v);
		if(m) Pr1MessageAddField(message.get(), MESSAGE_FIELD_ID_METADATA, m);

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
		lock_guard_t lock(bigLock_);

		if (!valid_)
			return;

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
			wrap_callback(
			[shared_this](const error_code& err, size_t sent)
			{
				shared_this->OnBinaryMessageSent(err, sent);
			}));
	}

	void TioTcpSession::OnBinaryMessageSent(const error_code& err, size_t sent)
	{
		if(CheckError(err))
		{
			//std::cerr << "ERROR sending binary data: " << err << std::endl;
			return;
		}

		lock_guard_t lock(bigLock_);

		DecreasePendingSendSize(sent);
		sentBytes_ += sent;

		BOOST_ASSERT(pendingSendSize_ >= 0);

		SendPendingBinaryData();
	}

	void TioTcpSession::RegisterLowPendingBytesCallback(std::function<void(shared_ptr<TioTcpSession>)> lowPendingBytesThresholdCallback)
	{
		lock_guard_t lock(bigLock_);

		BOOST_ASSERT(IsPendingSendSizeTooBig());

		lowPendingBytesThresholdCallbacks_.push(lowPendingBytesThresholdCallback);
		logstream_ << "RegisterLowPendingBytesCallback, " << lowPendingBytesThresholdCallbacks_.size() << " callbacks" << endl;
	}

	void TioTcpSession::DecreasePendingSendSize(int size)
	{
		lock_guard_t lock(bigLock_);

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
		lock_guard_t lock(bigLock_);

		if (!valid_)
			return;

		pendingBinarySendData_.push_back(message);

		IncreasePendingSendSize(pr1_message_get_data_size(message.get()));

		SendPendingBinaryData();
	}

	void TioTcpSession::SendBinaryMessages(const vector<shared_ptr<PR1_MESSAGE>>& messages)
	{
		lock_guard_t lock(bigLock_);

		if (!valid_)
			return;

		for (const auto& message : messages)
		{
			pendingBinarySendData_.push_back(message);
			IncreasePendingSendSize(pr1_message_get_data_size(message.get()));
		}		

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


