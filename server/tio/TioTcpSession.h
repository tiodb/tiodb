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
#include "Command.h"
#include "../../client/c/tioclient_internals.h"
#include "HttpParser.h"
//#include "TioTcpServer.h"

namespace tio
{
	using std::endl;
	using std::atomic;

	inline TioData Pr1MessageToCppTioData(const PR1_MESSAGE_FIELD_HEADER* field)
	{
		TioData ret;
		char* stringBuffer;

		switch(field->data_type)
		{
		case MESSAGE_FIELD_TYPE_NONE:
			break;
		case TIO_DATA_TYPE_STRING:
			stringBuffer = (char*) (&field[1]);
			ret.Set(stringBuffer, field->data_size);
			break;
		case TIO_DATA_TYPE_INT:
			ret.Set(pr1_message_field_get_int(field));
			break;
		case TIO_DATA_TYPE_DOUBLE:
			ret.Set(pr1_message_field_get_double(field));
			break;
		};

		return ret;
	}

	inline void Pr1MessageAddField(PR1_MESSAGE* message, unsigned short fieldId, int value)
	{
		pr1_message_add_field_int(message, fieldId, value);
	}

	inline void Pr1MessageAddField(PR1_MESSAGE* message, unsigned short fieldId, const string& value)
	{
		pr1_message_add_field_string(message, fieldId, value.c_str());
	}

	inline void Pr1MessageAddField(PR1_MESSAGE* message, unsigned short fieldId, const TioData& tiodata)
	{
		unsigned short fieldType;

		switch(tiodata.GetDataType())
		{
		case TioData::String:
			fieldType = TIO_DATA_TYPE_STRING;
			break;
		case TioData::Int:
			fieldType = TIO_DATA_TYPE_INT;
			break;
		case TioData::Double:
			fieldType = TIO_DATA_TYPE_DOUBLE;
			break;
		default:
			fieldType = TIO_DATA_TYPE_NONE;
			pr1_message_add_field(message, fieldId, fieldType, NULL, 0);
			return;
		}

		pr1_message_add_field(message, fieldId, fieldType, tiodata.AsRaw(), tiodata.GetSize());
	}


inline bool Pr1MessageGetField(const PR1_MESSAGE* message, unsigned int fieldId, TioData* tiodata)
	{
		PR1_MESSAGE_FIELD_HEADER* field = pr1_message_field_find_by_id(message, fieldId);

		if(!field)
			return false;

		*tiodata = Pr1MessageToCppTioData(field);

		return true;
	}

	inline bool Pr1MessageGetField(const PR1_MESSAGE* message, unsigned int fieldId, string* str)
	{
		PR1_MESSAGE_FIELD_HEADER* field = pr1_message_field_find_by_id(message, fieldId);

		if(!field)
			return false;

		if(field->data_type != TIO_DATA_TYPE_STRING)
			return false;

		char* stringBuffer = (char*) (&field[1]);
		*str = string(stringBuffer, stringBuffer + field->data_size);

		return true;
	}

	inline bool Pr1MessageGetField(const PR1_MESSAGE* message, unsigned int fieldId, int* value)
	{
		PR1_MESSAGE_FIELD_HEADER* field = pr1_message_field_find_by_id(message, fieldId);

		if(!field)
			return false;

		if(field->data_type != TIO_DATA_TYPE_INT)
			return false;

		*value = pr1_message_field_get_int(field);

		return true;
	}
	
	inline void Pr1MessageGetHandleKeyValueAndMetadata(const PR1_MESSAGE* message, int* handle, TioData* key, TioData* value, TioData* metadata)
	{
		if(handle)
		{
			*handle = 0;
			Pr1MessageGetField(message, MESSAGE_FIELD_ID_HANDLE, handle);
		}

		if(key)
			Pr1MessageGetField(message, MESSAGE_FIELD_ID_KEY, key);

		if(value)
			Pr1MessageGetField(message, MESSAGE_FIELD_ID_VALUE, value);

		if(metadata)
			Pr1MessageGetField(message, MESSAGE_FIELD_ID_METADATA, metadata);
	}

	inline shared_ptr<PR1_MESSAGE> Pr1CreateMessage()
	{
		return shared_ptr<PR1_MESSAGE>(pr1_message_new(), &pr1_message_delete);
	}
	
	inline shared_ptr<PR1_MESSAGE> Pr1CreateAnswerMessage()
	{
		shared_ptr<PR1_MESSAGE> answer = Pr1CreateMessage();

		pr1_message_add_field_int(answer.get(), MESSAGE_FIELD_ID_COMMAND, TIO_COMMAND_ANSWER);

		return answer;
	}

	inline void Pr1MessageAddFields(shared_ptr<PR1_MESSAGE> message, const TioData* key, const TioData* value, const TioData* metadata)
	{
		if(key && key->GetDataType() != TioData::None)
			Pr1MessageAddField(message.get(), MESSAGE_FIELD_ID_KEY, *key);

		if(value && value->GetDataType() != TioData::None)
			Pr1MessageAddField(message.get(), MESSAGE_FIELD_ID_VALUE, *value);

		if(metadata && metadata->GetDataType() != TioData::None)
			Pr1MessageAddField(message.get(), MESSAGE_FIELD_ID_METADATA, *metadata);
	}

	inline shared_ptr<PR1_MESSAGE> Pr1CreateAnswerMessage(TioData* key, TioData* value, TioData* metadata)
	{
		shared_ptr<PR1_MESSAGE> answer = Pr1CreateAnswerMessage();
					
		Pr1MessageAddFields(answer, key, value, metadata);

		return answer;
	}

	
	using std::shared_ptr;
	using std::weak_ptr;
	using boost::system::error_code;
	using std::stringstream;

	namespace asio = boost::asio;
	using namespace boost::asio::ip;

	class TioTcpServer;

	struct HandleInfo
	{
		shared_ptr<ITioContainer> container;
		
		HandleInfo(const shared_ptr<ITioContainer> container)
			: container(container)
		{}
	};

	class TioTcpSession : 
		public std::enable_shared_from_this<TioTcpSession>,
		public boost::noncopyable
	{
	private:
		unsigned int id_;

		asio::io_service& io_service_;
		tcp::socket socket_;
		TioTcpServer& server_;

		Command currentCommand_;

		asio::streambuf buf_;

		typedef std::map<unsigned int, HandleInfo> HandleMap;

		HandleMap handles_;
		
		unsigned int lastHandle_;
		int sentBytes_;
        int pendingSendSize_;
		int maxPendingSendingSize_;

		bool binaryProtocol_;

		static std::ostream& logstream_;

		std::queue<std::function<void (shared_ptr<TioTcpSession>)>> lowPendingBytesThresholdCallbacks_;

        std::queue<std::string> pendingSendData_;

		typedef tio_recursive_mutex mutex_t;
		typedef lock_guard<mutex_t> lock_guard_t;

		mutex_t bigLock_;
		
		std::list< shared_ptr<PR1_MESSAGE> > pendingBinarySendData_;
		std::vector< asio::const_buffer > beingSendData_;
		shared_ptr<char> binarySendBuffer_;

		vector<string> tokens_;

		bool valid_;

		static int PENDING_SEND_SIZE_BIG_THRESHOLD;
		static int PENDING_SEND_SIZE_SMALL_THRESHOLD;

		void SendString(const string& str);
		void SendStringNow(const string& str);
		
        void UnsubscribeAll();

		void OnBinaryProtocolMessage(PR1_MESSAGE* message, const error_code& err);
		void OnBinaryProtocolMessageHeader(shared_ptr<PR1_MESSAGE_HEADER> header, const error_code& err);
		void ReadHttpCommand(const shared_ptr<HttpParser> httpParser);
		void ReadBinaryProtocolMessage();

		typedef function<void(const error_code&, size_t)> AsioCallbackT;

		AsioCallbackT wrap_callback(AsioCallbackT cb)
		{
			return cb;
			//return strand_.wrap(cb);
		}


	public:

		void SendResultSetItem(unsigned int queryID, 
			const TioData& key, const TioData& value, const TioData& metadata);

		void SendBinaryErrorAnswer(int errorCode, const string& description);

		void SendPendingBinaryData();

		void OnBinaryMessageSent(const error_code& err, size_t sent);

		void IncreasePendingSendSize(int size)
		{
			pendingSendSize_ += size;

			if(pendingSendSize_ > maxPendingSendingSize_)
				maxPendingSendingSize_ = pendingSendSize_;
		}


		void RegisterLowPendingBytesCallback(std::function<void (shared_ptr<TioTcpSession>)> lowPendingBytesThresholdCallback);

		void DecreasePendingSendSize(int size);

		int pendingSendSize()
		{
			return pendingSendSize_;
		}

		bool IsPendingSendSizeTooBig()
		{
			return pendingSendSize_ > PENDING_SEND_SIZE_BIG_THRESHOLD;
		}

		void SendBinaryMessage(const shared_ptr<PR1_MESSAGE>& message);

		void SendBinaryAnswer(TioData* key, TioData* value, TioData* metadata);

		void SendBinaryAnswer();

		TioTcpSession(asio::io_service& io_service, TioTcpServer& server, unsigned int id);
		~TioTcpSession();
		void LoadDispatchMap();

		tcp::socket& GetSocket();
		void OnAccept();
		void ReadCommand();

		void OnReadHttpCommand(const shared_ptr<HttpParser>& httpParser, const error_code& err, size_t read);
		unsigned int id();
		bool UsesBinaryProtocol() const;

		void SendResultSet(shared_ptr<ITioResultSet> resultSet, unsigned int queryID);

		void SendResultSetStart(unsigned int queryID);
		void SendResultSetEnd(unsigned int queryID);

		bool IsValid();

		void OnReadCommand(const error_code& err, size_t read);
		void OnWrite(char* buffer, size_t bufferSize, const error_code& err, size_t read);
		void OnReadMessage(const error_code& err);
		bool CheckError(const error_code& err);
		void OnCommandData(size_t dataSize, const error_code& err, size_t read);
		void SendAnswer(stringstream& answer);
		void SendAnswer(const string& answer);

		unsigned int RegisterContainer(const string& containerName, shared_ptr<ITioContainer> container);
		shared_ptr<ITioContainer> GetRegisteredContainer(unsigned int handle, string* containerName = NULL, string* containerType = NULL);
		void CloseContainerHandle(unsigned int handle);

		void SendTextEvent(unsigned int handle, const TioData& key, const TioData& value, const TioData& metadata, const string& eventName);

		void Subscribe(unsigned int handle, const string& start)
		{
			auto container = GetRegisteredContainer(handle);

			auto snapshot = container->Query(0, -1, nullptr);
		}

		void BinarySubscribe(unsigned int handle, const string& start, bool sendAnswer)
		{

		}
		void Unsubscribe(unsigned int handle)
		{

		}

		const vector<string> GetTokens();
		void AddToken(const string& token);
		
		void SetCommandRunning()
		{
			commandRunning_ = true;
		}
		void UnsetCommandRunning()
		{
			commandRunning_ = false;
		}
		void OnContainerEvent(ContainerEvent eventId, const TioData & k, const TioData & v, const TioData & m);
		void SendBinaryEvent(int handle, const TioData& key, const TioData& value, const TioData& metadata, const string& eventName);
		void SendBinaryResultSet(shared_ptr<ITioResultSet> resultSet, unsigned int queryID, function<bool(const TioData& key)> filterFunction, unsigned maxRecords);

		bool commandRunning_;

		void InvalidateConnection(const error_code& err);

		void SendHttpResponseAndClose(int statusCode, const string& statusMessage, const string& mimeType, const map<string, string>* responseHeaders, const string& body);
	};		
}
