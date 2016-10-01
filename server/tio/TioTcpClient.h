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
#include <iostream>
#include <iomanip>
#include <sstream>
#include <numeric>
#include <queue>
#include <assert.h>
#include <deque>
#include <limits>
#include <map>
#include <vector>
#include <boost/function.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/noncopyable.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/join.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/asio.hpp>
#include <boost/foreach.hpp>

 #define min(x,y) (x<y?x:y)




#ifndef INTERFACE
#define INTERFACE struct
#endif

#ifndef ASSERT
#define ASSERT assert
#endif

#include "Container.h"
#include "buffer.h"
#include "TioTcpProtocol.h"

#include <boost/thread/thread.hpp>

namespace tio
{
	
	using std::shared_ptr;
	using boost::system::error_code;
	namespace asio = boost::asio;
	using boost::asio::ip::tcp;
	namespace ip = boost::asio::ip;

	using std::queue;
	using std::string;

	struct Event
	{
		unsigned int handle;
		string name;
		TioData key, value, metadata;
	};

	class RemoteContainerManager
	{
	private:
		asio::io_service& io_service_;
		tcp::socket socket_;
		
		asio::streambuf response_;
		std::istream response_stream_;
		
		string serverVersion_;

		queue<Event> pendingEvents_;

		typedef std::map<unsigned int, EventDispatcher> DispatcherMap;
		DispatcherMap dispatchers_;

		typedef std::map< unsigned int, queue<EventSink> > PoppersMap;
		PoppersMap poppers_;

		typedef std::map< unsigned int, std::map< std::string, queue<EventSink> > > KeyPoppersMap;
		KeyPoppersMap keyPoppers_;

		shared_ptr<ITioContainer> CreateOrOpenContainer(const string& command, const string& type, const string& name);

	public:
		RemoteContainerManager(asio::io_service& io_service);
		size_t PendingEventsCount();

		void Connect(const char* host, unsigned short port);
			
		string ReceiveLine();

		inline void ReceiveDataAnswer(vector<string>::iterator begin, vector<string>::iterator end, ProtocolAnswer* answer);
		
		void HandleEventAnswer(ProtocolAnswer* answer);

		unsigned int Subscribe(unsigned int handle, EventSink sink, const string& start);
		void Unsubscribe(unsigned int handle, unsigned int cookie);

		void WaitAndPop_Next(shared_ptr<ITioContainer> container, EventSink sink);
		void WaitAndPop_Key(shared_ptr<ITioContainer> container, EventSink sink, const TioData& key);
		
		void ReceiveIfAny(ProtocolAnswer* answer);
		void Receive(ProtocolAnswer* answer);
		void ReceiveUntilAnswer(ProtocolAnswer* answer);
		inline void SendCommand(const string& command, const string parameter, ProtocolAnswer* answer = NULL,
			const TioData& key = TIONULL, const TioData& value = TIONULL, const TioData& metadata = TIONULL);

		shared_ptr<ITioContainer> CreateContainer(const string& type, const string& name);
		shared_ptr<ITioContainer> OpenContainer(const string& type, const string& name);
		void ReceiveAndDispatchEvents();
	};

	class RemoteContainer : public ITioContainer
	{
		RemoteContainerManager& manager_;
		string handle_;

		void ThrowAnswerIfError(const ProtocolAnswer& answer);
		void SendCommand(const string& command, ProtocolAnswer* answer, 
			const TioData& key = TIONULL, const TioData& value = TIONULL, const TioData& metadata = TIONULL); 

		void SendDataCommand(const string& command, 
			const TioData& key, const TioData& value, const TioData& metadata);

		void SendDataCommand(const string& command, 
			const TioData& key, TioData* value = NULL, TioData* metadata = NULL);

		void SendDataCommand(const string& command, 
			TioData* key, TioData* value = NULL, TioData* metadata = NULL);

		inline void SendOutputDataCommand(const string& command, 
			TioData* key, TioData* value, TioData* metadata);

	public:

		RemoteContainer(RemoteContainerManager& manager, string handle);
		~RemoteContainer();

		string GetHandle();

		virtual size_t GetRecordCount();

		virtual void PushBack(const TioData& key, const TioData& value, const TioData& metadata);
		virtual void PushFront(const TioData& key, const TioData& value, const TioData& metadata);

		virtual void PopBack(TioData* key, TioData* value, TioData* metadata);
		virtual void PopFront(TioData* key, TioData* value, TioData* metadata);

		virtual void GetRecord(const TioData& searchKey, TioData* key, TioData* value, TioData* metadata);

		virtual void Set(const TioData& key, const TioData& value, const TioData& metadata);
		virtual void Insert(const TioData& key, const TioData& value, const TioData& metadata);
		virtual void Delete(const TioData& key, const TioData& value, const TioData& metadata);
		virtual void Clear();
		virtual shared_ptr<ITioResultSet> Query(int startOffset, int endOffset, const TioData& query);

		virtual string GetType();
		virtual string GetName();

		virtual string Command(const string& command);

		virtual void SetProperty(const string& key, const string& value);
		virtual string GetProperty(const string& key);

		virtual unsigned int Subscribe(EventSink sink, const string& start);
		virtual void Unsubscribe(unsigned int cookie);

		virtual void Modify(const TioData& key, TioData* value);

	};

	class FieldParser
	{
		vector<string> keys_;
		typedef map<string, string> FieldsMap;
		FieldsMap fields_;
	public:
		FieldParser(const string& schema)
		{
			SetSchema(schema);
		}

		FieldParser() {}

		const vector<string>& GetKeys() const
		{
			return keys_;
		}

		void SetSchema(const string& schema)
		{
			ClearValues();

			boost::algorithm::split(
				keys_, 
				schema, 
				boost::algorithm::is_any_of("^"));
		}

		bool Empty() const
		{
			return fields_.empty();
		}

		/*void Load(const TioData& value)
		{
			Load(value.AsSz());
		}*/

		void Load(const string& value)
		{
			vector<string> loadVector;
			boost::algorithm::split(loadVector, value, boost::algorithm::is_any_of("^"));

			if(loadVector.size() != keys_.size())
				throw std::invalid_argument("value not valid");

			for(size_t a = 0 ; a < loadVector.size() ; a++)
				fields_[keys_[a]] = loadVector[a];
		}

		const string& GetField(string key) const
		{
			FieldsMap::const_iterator i = fields_.find(key);

			if(i == fields_.end())
				throw std::invalid_argument(string("field not found: ") + key);
			
			return i->second;
		}

		template<typename T>
		T GetField(string key) const
		{
			return lexical_cast<T>(GetField(key));
		}

		/*
		void SetField(const string& key, const string& value)
		{
			if(std::find(keys_.begin(), keys_.end(), key) == keys_.end())
				throw std::runtime_error("invalid key");

			fields_[key] = value;
		}
		*/

		void SetField(const string& key, const TioData& value)
		{
			if(std::find(keys_.begin(), keys_.end(), key) == keys_.end())
				throw std::runtime_error("invalid key");
			
			switch(value.GetDataType())
			{
			case TioData::Sz:
				fields_[key] = value.AsSz();
				break;
			case TioData::Int:
				fields_[key] = lexical_cast<string>(value.AsInt());
				break;
			case TioData::Double:
				fields_[key] = lexical_cast<string>(value.AsDouble());
				break;
			default:
				fields_[key].clear();
			}
		}

		void Serialize(TioData* data) const
		{
			if(keys_.empty())
				return;

			stringstream buffer;
			
			BOOST_FOREACH(const string& key, keys_)
			{	
				FieldsMap::const_iterator i = fields_.find(key);
				
				if(i != fields_.end())
					buffer << i->second;
					
				buffer << "^";
			}

			//
			// delete the last ^
			//
			const string& str = buffer.str();

			data->Set(str.substr(0,str.length() - 1));
		}

		void CopyFrom(const FieldParser& from)
		{
			BOOST_FOREACH(const string& key, from.GetKeys())
			{
				if(std::find(keys_.begin(), keys_.end(), key) != keys_.end())
					fields_[key] = from.GetField(key);
			}
		}

		void ClearValues()
		{
			fields_.clear();
		}
	};

	/*

	class TioAsyncClient
	{
	public:

		typedef TioAsyncClient ThisType;

		typedef std::function<void (error_code)> ConnectCallback;
		typedef std::function<void (error_code, ProtocolAnswer*)> AnswerCallback;

		tcp::socket socket_;
		asio::streambuf buffer_;
		typedef map<string, AnswerCallback> SubscribersMap;
		SubscribersMap subscribers_;

		queue<AnswerCallback> callbackQueue_;

		TioAsyncClient(asio::io_service& io_service)
			: socket_(io_service)
		{

		}

		void PushCallback(AnswerCallback callback)
		{
			callbackQueue_.push(callback);
		}

		void WrongWrongWrongNoCallbackInTheQueue(error_code err, ProtocolAnswer* answer)
		{
			BOOST_ASSERT(false && "not supposed to happen");
		}

		AnswerCallback PopNextCallback()
		{
			AnswerCallback callback;

			if(!callbackQueue_.empty())
			{
				callback = callbackQueue_.front();
				callbackQueue_.pop();
			}	
			else
				callback = boost::bind(&ThisType::WrongWrongWrongNoCallbackInTheQueue, this, _1, _2);

			return callback;
		}

		void Connect(const string& host, unsigned short port, ConnectCallback callback)
		{
			socket_.async_connect(
				ip::tcp::endpoint(ip::address_v4::from_string(host), port),
				boost::bind(&TioAsyncClient::_OnConnect, this, callback, asio::placeholders::error));
		}

		void _OnConnect(ConnectCallback callback, error_code err)
		{
			boost::asio::detail::throw_error(err);
			callback(err);
			AsyncReadLine();
		}

		void SendCommand(const string& command, const string& parameter, 
			AnswerCallback callback,
			const TioData& key = TIONULL,
			const TioData& value = TIONULL,
			const TioData& metadata = TIONULL)
		{
			SendRawCommand(GenerateCommand(command, parameter, key, value, metadata), callback);			
		}

		void SendRawCommand(const string& command, AnswerCallback callback)
		{
			size_t bufferSize = command.size();
			std::shared_ptr<char> buf(new char[bufferSize]);

			memcpy(buf.get(), command.c_str(), command.size());

			PushCallback(callback);

			asio::async_write(socket_, asio::buffer(buf.get(), bufferSize),
				boost::bind(&TioAsyncClient::_OnCommandSent, this, buf, 
				asio::placeholders::bytes_transferred, asio::placeholders::error));
		}

		void CreateContainer(const string& type, const string& name, AnswerCallback callback)
		{
			SendCommand("create_container", type + " " +  name, callback);
		}

		void OpenContainer(const string& type, const string& name, AnswerCallback callback)
		{
			SendCommand("open_container", type + " " +  name, callback);
		}

		void OpenContainerAndGetValue(const string& type, const string& name, const string& key,
			AnswerCallback answerCallback)
		{
			OpenContainer(type, name, 
				boost::bind(&ThisType::_ThenGetValue, this, answerCallback, key, _1, _2));
		}

		void CreateContainerAndGetValue(const string& type, const string& name, const string& key,
			AnswerCallback answerCallback)
		{
			CreateContainer(type, name, 
				boost::bind(&ThisType::_ThenGetValue, this, answerCallback, key, _1, _2));
		}

		void CreateContainerAndSubscribe(const string& type, const string& name, const string& startIndex,
			AnswerCallback answerCallback, AnswerCallback eventsCallback)
		{
			CreateContainer(type, name, 
				boost::bind(&ThisType::_ThenSubscribe, this, answerCallback, eventsCallback, startIndex, _1, _2));
		}

		void _ThenGetValue(AnswerCallback answerCallback, const string& key,
			error_code err, ProtocolAnswer* answer)
		{
			if(err || answer->error)
			{
				answerCallback(err, answer);
				return;
			}

			BOOST_ASSERT(answer->parameterType == "handle");

			const string& handle = answer->parameter;

			SendCommand("get", handle, answerCallback, shared_ptr<TioData>(new TioData(key)));
			SendCommand("close_container", handle, boost::bind(&ThisType::JustLog, this, _1, _2));
		}

		void JustLog(error_code err, ProtocolAnswer* answer)
		{

		}

		void _ThenSubscribe(AnswerCallback answerCallback, AnswerCallback eventsCallback, const string& startIndex,
			error_code err, ProtocolAnswer* answer)
		{
			if(err || answer->error)
			{
				answerCallback(err, answer);
				return;
			}

			BOOST_ASSERT(answer->parameterType == "handle");

			const string& handle = answer->parameter;

			Subscribe(handle, startIndex, 
				boost::bind(&ThisType::_OpenEndStep, this, handle, answerCallback, _1, _2), eventsCallback);
		}

		void _OpenEndStep(const string& handle, AnswerCallback answerCallback, 
			error_code err, ProtocolAnswer* answer)
		{
			if(err || answer->error)
			{
				answerCallback(err, answer);
				return;
			}

			//
			// we'll create a fake answer, to send the handle via callback
			//
			ProtocolAnswer fakeAnswer;
			fakeAnswer.type = ProtocolAnswer::TypeAnswer;
			fakeAnswer.parameterType = "handle";
			fakeAnswer.parameter = handle;
			fakeAnswer.error = false;

			answerCallback(err, &fakeAnswer);
		}

		void _OnSubscribe(const string& handle, AnswerCallback answerCallback, AnswerCallback eventsCallback,
			error_code err, ProtocolAnswer* answer)
		{
			if(err || answer->error)
			{
				subscribers_.erase(handle);
				answerCallback(err, answer);
				return;
			}

			answerCallback(err, answer);
		}

		void Subscribe(const string& handle, const string& startIndex, 
			AnswerCallback answerCallback, AnswerCallback eventsCallback)
		{
			SendCommand("subscribe", handle + (startIndex.empty() ? "" : " " + startIndex),
				boost::bind(&TioAsyncClient::_OnSubscribe, this, handle, answerCallback, eventsCallback, _1, _2));

			//
			// we'll subscribe now, because events can come
			// before OK.
			//
			subscribers_[handle] = eventsCallback;
		}

		string GenerateCommand(const string& command, const string& parameter,
			const TioData& key,
			const TioData& value,
			const TioData& metadata)
		{
			stringstream cmdStream;

			cmdStream << command;

			if(!parameter.empty())
				cmdStream << " " << parameter; 

			if(!key && !value && !metadata)
				cmdStream << "\r\n";
			else
				SerializeData(key, value, metadata, cmdStream);

			return cmdStream.str();
		}

		void _OnCommandSent(std::shared_ptr<char> buf, size_t bytes_transferred, error_code err)
		{
			if(err)
			{
				PopNextCallback()(err, NULL);
				return;
			}
		}

		void AsyncReadLine()
		{
			asio::async_read_until(socket_, buffer_, '\n', 
				boost::bind(&TioAsyncClient::OnReadLine, this, 
					asio::placeholders::error, asio::placeholders::bytes_transferred) );
		}

		void _OnReceiveDataAnswer(shared_ptr<ProtocolAnswer> answer, size_t receivedBytes, error_code err)
		{
			if(err)
			{
				PopNextCallback()(err, NULL);

				//
				// in case of tcp error, we can't ask for more data,
				// so we'll not call AsyncReadLine
				//
				return;
			}

			ExtractFieldsFromBuffer(
				answer->fieldSet, 
				asio::buffer_cast<const void*>(buffer_.data()),
				asio::buffer_size(buffer_.data()),
				&answer->key, &answer->value, &answer->metadata);

			buffer_.consume(answer->pendingDataSize);

			if(answer->type == ProtocolAnswer::TypeEvent)
				DispatchEvent(*answer);
			else
				PopNextCallback()(err, answer.get());

			AsyncReadLine();
		}


		void OnReadLine(error_code err, size_t read)
		{
			if(err)
			{
				PopNextCallback()(err, NULL);
				return;
			}

			shared_ptr<ProtocolAnswer> answer = shared_ptr<ProtocolAnswer>(new ProtocolAnswer());

			string answerLine;
			istream stream (&buffer_);

			getline(stream, answerLine);

			ParseAnswerLine(answerLine, answer.get());

			if(answer->pendingDataSize == 0)
			{
				PopNextCallback()(err, answer.get());
				AsyncReadLine();
				return;
			}

			if(buffer_.size() < answer->pendingDataSize)
				asio::async_read(socket_, buffer_, asio::transfer_at_least(answer->pendingDataSize - buffer_.size()),
					boost::bind(&TioAsyncClient::_OnReceiveDataAnswer, this, answer, 
					asio::placeholders::bytes_transferred, asio::placeholders::error) );
			else
				_OnReceiveDataAnswer(answer, buffer_.size(), err);
		}

		void DispatchEvent(ProtocolAnswer& answer)
		{
			BOOST_ASSERT(answer.type == ProtocolAnswer::TypeEvent);
			const string handle = answer.parameter;

			SubscribersMap::iterator i = subscribers_.find(handle);

			if(i == subscribers_.end())
			{
				BOOST_ASSERT(false);
			}

			AnswerCallback& callback = i->second;

			callback(error_code(), &answer);
		}

	};

	
	class AsyncTioClient
	{
		boost::thread_group threadGroup_;
		boost::thread* thread_;
		boost::mutex runningMutex_;

		asio::io_service io_service_;
		tcp::socket socket_;
		asio::streambuf buffer_;
		std::queue< std::function<void (const ProtocolAnswer&)> > answerCallbackQueue_;
		boost::mutex answerCallbackQueueMutex_;
		std::function<void (const ProtocolAnswer&)> eventCallback_;
		ProtocolAnswer currentAnswer_;
		bool started_;

		void _Connect(std::function<void(void)> callback, const string& host, unsigned short port)
		{
			socket_.async_connect(
				ip::tcp::endpoint(ip::address_v4::from_string(host), port),
				boost::bind(&AsyncTioClient::_OnConnect, this, callback, asio::placeholders::error));
		}

		void _SendCommand(const string& command, const string& parameter, 
			shared_ptr<TioData> key, shared_ptr<TioData> value, shared_ptr<TioData> metadata)
		{
			stringstream cmdStream;

			cmdStream << command;

			if(!parameter.empty())
				cmdStream << " " << parameter; 

			if(!key && !value && !metadata)
				cmdStream << "\r\n";
			else
				SerializeData(key.get(), value.get(), metadata.get(), cmdStream);

			string stringBuffer = cmdStream.str();
			size_t bufferSize = stringBuffer.size();

			char* buf = new char[bufferSize];

			memcpy(buf, stringBuffer.c_str(), bufferSize);

			asio::async_write(socket_, asio::buffer(buf, bufferSize),
				boost::bind(&AsyncTioClient::_OnCommandSent, this, asio::placeholders::error, buf));
		}

		
		void CallAnswerCallback()
		{
			if(currentAnswer_.isEvent)
				eventCallback_(currentAnswer_);
			else
			{
				std::function<void (const ProtocolAnswer&)> f;

				{
					boost::mutex::scoped_lock l(answerCallbackQueueMutex_);
					BOOST_ASSERT(!answerCallbackQueue_.empty());
					f = answerCallbackQueue_.front();
					answerCallbackQueue_.pop();
				}

				f(currentAnswer_);
			}
		}


		void OnReadLine(error_code err, size_t read)
		{
			boost::asio::detail::throw_error(err);

			vector<string> params;
			vector<string>::iterator current;
			string answerType, answerLine;
			istream stream (&buffer_);

			size_t preArea = buffer_.size();

			getline(stream, answerLine);

			size_t posArea = buffer_.size();

			if(*(answerLine.end() - 1) == '\r')
				answerLine.erase(answerLine.end() - 1);

		

			boost::split(
				params, 
				answerLine, 
				boost::is_any_of(" "));

			if(params.size() < 2)
				throw std::runtime_error("invalid answer from server");

			current = params.begin();

			answerType = *current++;

			if(answerType == "answer")
			{
				currentAnswer_.isEvent = false;

				currentAnswer_.result = *current++;

				if(currentAnswer_.result != "ok")
				{
					currentAnswer_.parameter = answerLine;
					CallAnswerCallback();
					AsyncReadLine();
					return;
				}

				//
				// simple answer, no data
				//
				if(current == params.end())
				{
					CallAnswerCallback();
					AsyncReadLine();
					return;
				}
					

				answerType = *current++;

				//
				// maybe a final space...
				//
				if(answerType.empty())
				{
					CallAnswerCallback();
					AsyncReadLine();
					return;
				}

				if(answerType == "data")
				{
					ReceiveDataAnswer(current, params.end());
				}
				else if(answerType == "handle" || answerType == "count" || answerType == "type")
				{
					currentAnswer_.parameterType = answerType;

					if(current == params.end())
						return;

					currentAnswer_.parameter = *current++;

					CallAnswerCallback();
					AsyncReadLine();
					return;
				}
			}
			else if(answerType == "event")
			{
				currentAnswer_.isEvent = true;

				currentAnswer_.parameterType = "handle";

				if(current == params.end())
					throw std::runtime_error("invalid answer from server");

				currentAnswer_.parameter = *current++;

				if(current == params.end())
					throw std::runtime_error("invalid answer from server");

				currentAnswer_.eventName = *current++;
				currentAnswer_.isEvent = true;

				ReceiveDataAnswer(current, params.end());
			}
		}

		void OnReceiveDataAnswer(const vector<FieldInfo>& fields, size_t fieldsTotalSize, size_t receivedBytes)
		{
			ExtractFieldsFromBuffer(
				fields, 
				asio::buffer_cast<const void*>(buffer_.data()),
				asio::buffer_size(buffer_.data()),
				&currentAnswer_.key, &currentAnswer_.value, &currentAnswer_.metadata);

			buffer_.consume(fieldsTotalSize);

			CallAnswerCallback();

			AsyncReadLine();
		}


		void ReceiveDataAnswer(vector<string>::iterator begin, vector<string>::iterator end)
		{
			size_t fieldsTotalSize;
			vector<FieldInfo> fields;

			pair_assign(fields, fieldsTotalSize) = ExtractFieldSet(begin, end);

			if(buffer_.size() < fieldsTotalSize)
				asio::async_read(socket_, buffer_, asio::transfer_at_least(fieldsTotalSize - buffer_.size()),
						boost::bind(&AsyncTioClient::OnReceiveDataAnswer, this, fields, fieldsTotalSize, 
						asio::placeholders::bytes_transferred) );
			else
				OnReceiveDataAnswer(fields, fieldsTotalSize, fieldsTotalSize);

			return;
		}

		void AsyncReadLine()
		{
			asio::async_read_until(socket_, buffer_, '\n', 
				boost::bind(&AsyncTioClient::OnReadLine, this, 
				asio::placeholders::error, asio::placeholders::bytes_transferred) );
		}
		void _OnCommandSent(error_code err, char* buffer)
		{
			boost::asio::detail::throw_error(err);

			delete buffer;
		}

		void _OnConnect(std::function<void(void)> callback, error_code err)
		{
			boost::asio::detail::throw_error(err);
			AsyncReadLine();
			callback();
		}

	public:

#ifdef _WIN32
		void SetWindowsEvent(HANDLE h)
		{
			SetEvent(h);
		}

		void Connect(const string& host, unsigned short port)
		{
			HANDLE h = CreateEvent(NULL, TRUE, FALSE, NULL);

			AsyncConnect(boost::bind(&AsyncTioClient::SetWindowsEvent, this, h), host, port);

			WaitForSingleObject(h, INFINITE);

			CloseHandle(h);
		}

		void CopyAnswerAndSetEvent(const ProtocolAnswer& serverAnswer, ProtocolAnswer* destinationAnswer, HANDLE h)
		{
			destinationAnswer->CopyFrom(serverAnswer);
			SetEvent(h);
		}

		void SendCommand(const string& command, const string& parameter, ProtocolAnswer* answer,
			TioData* key = NULL, TioData* value = NULL, TioData* metadata = NULL)
		{
			HANDLE h = CreateEvent(NULL, TRUE, FALSE, NULL);	

			AsyncSendCommand(boost::bind(&AsyncTioClient::CopyAnswerAndSetEvent, this, _1, answer, h),
				command, parameter, key, value, metadata);

			WaitForSingleObject(h, INFINITE);

			CloseHandle(h);
		}


#endif // _WIN32

		void AsyncConnect(std::function<void(void)> callback, const string& host, unsigned short port)
		{
			io_service_.post(
				boost::bind(&AsyncTioClient::_Connect, this, callback, host, port) );
			
			//
			// we need to start AFTER some work was sent to io_service,
			// because he will exit as soon as there's no more work to do
			//
			Start();
		}

		shared_ptr<TioData> CopyTioDataIfAny(TioData* data)
		{
			return shared_ptr<TioData>(data ? new TioData(data) : NULL);
		}

		void AsyncSendCommand(std::function<void (const ProtocolAnswer&)> callback, const string& command, const string& parameter,
			TioData* key = NULL, TioData* value = NULL, TioData* metadata = NULL)
		{
			{
				boost::mutex::scoped_lock l(answerCallbackQueueMutex_);
				answerCallbackQueue_.push(callback);
			}

			io_service_.post(
				boost::bind(&AsyncTioClient::_SendCommand, this, command, parameter, 
				CopyTioDataIfAny(key), CopyTioDataIfAny(value), CopyTioDataIfAny(metadata)));
		}

		AsyncTioClient() : 
		started_(false),
			io_service_(),
			socket_(io_service_)
		{}

		~AsyncTioClient()
		{
			Stop();
		}

		void Start()
		{
			if(started_)
				return;

			thread_ = threadGroup_.create_thread(boost::bind(&AsyncTioClient::ThreadProc, this));
			started_ = true;
		}

		void Stop()
		{
			if(!started_)
				return;

			io_service_.post(boost::bind(&asio::io_service::stop, &io_service_));

			//
			// wait for running thread to finish
			//
			boost::mutex::scoped_lock lock(runningMutex_);
		}


		void ThreadProc()
		{
			boost::mutex::scoped_lock lock(runningMutex_);
			io_service_.run();
		}

		void SetEventCallback(std::function<void (const ProtocolAnswer&)> callback)
		{
			eventCallback_ = callback;
		}
	};
	*/
}



