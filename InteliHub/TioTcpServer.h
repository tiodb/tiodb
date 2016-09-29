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

#include "pch.h"
#include "buffer.h"
#include "Command.h"
#include "ContainerManager.h"
#include "TioTcpProtocol.h"
#include "TioTcpSession.h"
#include "auth.h"
#include "logdb.h"

namespace tio
{
	
	using std::shared_ptr;
	using boost::system::error_code;
	namespace asio = boost::asio;
	using namespace boost::asio::ip;

	using std::string;
	using std::vector;
	using std::map;
	using std::deque;
	
	std::string Serialize(const std::list<const TioData*>& fields);


	class GroupManager : boost::noncopyable
	{
		struct SubscriberInfo
		{
			weak_ptr<TioTcpSession> session;
			string start;

			SubscriberInfo(){}

			SubscriberInfo(shared_ptr<TioTcpSession> session, string start)
				: session(session)
				, start(start)
			{
			}
		};

		class GroupInfo : boost::noncopyable
		{
			string groupName_;
			string containerListName_;
			shared_ptr<ITioContainer> containerListContainer_;
			typedef map<string, shared_ptr<ITioContainer>> ContainersMap;
			ContainersMap containers_;
			bool valid_; // we're using member functions as callback

			map<unsigned, SubscriberInfo> subscribers_;

			void SendNewContainerToSubscriber(const shared_ptr<ITioContainer> container, const shared_ptr<TioTcpSession>& session, const string& start)
			{
				if(!session->IsValid())
					return;

				string containerName = container->GetName();
				unsigned handle = session->RegisterContainer(containerName, container);

				if(session->UsesBinaryProtocol())
				{
					unsigned handle = session->RegisterContainer(containerName, container);

					auto answer = Pr1CreateMessage();

					Pr1MessageAddField(answer.get(), MESSAGE_FIELD_ID_COMMAND, TIO_COMMAND_NEW_GROUP_CONTAINER);
					Pr1MessageAddField(answer.get(), MESSAGE_FIELD_ID_HANDLE, handle);
					Pr1MessageAddField(answer.get(), MESSAGE_FIELD_ID_GROUP_NAME, groupName_);
					Pr1MessageAddField(answer.get(), MESSAGE_FIELD_ID_CONTAINER_NAME, containerName);
					Pr1MessageAddField(answer.get(), MESSAGE_FIELD_ID_CONTAINER_TYPE, container->GetType());

					session->SendBinaryMessage(answer);

					session->BinarySubscribe(handle, start, false);
				}
				else
				{
					string answer = "group_container ";
					answer += groupName_;
					answer += " ";
					answer += containerName;
					answer += " ";
					answer += container->GetType();
					answer += " ";
					answer += lexical_cast<string>(handle);
					answer += "\r\n";

					session->SendAnswer(answer);
					session->Subscribe(handle, start, -1, false);
				}
			}

		public:

			GroupInfo(ContainerManager* containerManager, const string& name)
				: groupName_(name), valid_(true)
			{
				containerListName_ = "__meta__/groups/";
				containerListName_ += name;

				containerListContainer_ = containerManager->CreateContainer("volatile_map", containerListName_);
			}

			~GroupInfo()
			{
				valid_ = false;
			}

			GroupInfo(GroupInfo&& rhv)
			{
				std::swap(groupName_, rhv.groupName_);
				std::swap(containerListName_, rhv.containerListName_);
				std::swap(containerListContainer_, rhv.containerListContainer_);
				std::swap(containers_, rhv.containers_);
				std::swap(subscribers_, rhv.subscribers_);
			}

			void AddContainer(shared_ptr<ITioContainer> container)
			{
				if(containers_.find(container->GetName()) != containers_.end())
					return;

				containers_[container->GetName()] = container;
				containerListContainer_->Set(container->GetName(), groupName_);

				for(auto& p: subscribers_)
				{
					SubscriberInfo& subscriberInfo = p.second;
					auto session = subscriberInfo.session.lock();
					
					if(!session || ! session->IsValid())
						continue;

					//
					// TODO: respect start parameter, we need to save this with session ptr
					//
					SendNewContainerToSubscriber(container, session, subscriberInfo.start);
				}
			}

			void Subscribe(const shared_ptr<TioTcpSession>& session, const string& start)
			{
				session->RegisterContainer(containerListName_, containerListContainer_);

				if(session->UsesBinaryProtocol())
				{
					session->SendBinaryAnswer();
				}
				else
				{
					session->SendAnswer("answer ok\r\n");
				}

				for(auto i = containers_.begin() ; i != containers_.end() ; ++i)
				{
					SendNewContainerToSubscriber(i->second, session, start);
				}

				subscribers_[session->id()] = SubscriberInfo(session, start);
			}

			/*
			void DoPendingSubscriptions(const shared_ptr<TioTcpSession>& session, ContainersMap::const_iterator i, const string& start)
			{
				if(i == containers_.begin())
					std::cout << "DoPendingSubscriptions start, " << containers_.size() << " containers" << std::endl; 

				Timer timer;
				timer.Start();
				int howMany = 0;

				for( ; i != containers_.end() ; ++i)
				{
					SendNewContainerToSubscriber(i->second, session, start);
					howMany++;

					if(session->IsPendingSendSizeTooBig())
					{
						session->RegisterLowPendingBytesCallback(
							[this, i, start](const shared_ptr<TioTcpSession>& session)
							{
								BOOST_ASSERT(valid_);
								// don't know why, but ++i triggers a const error...
								auto i2 = i;
								++i2;
								this->DoPendingSubscriptions(session, i2, start);
							});

						return;
					}
				}

				long long elapsedMicro = timer.ElapsedInMicroseconds();
				long long elapsedInSeconds = elapsedMicro / (1000 * 1000);
				double persec = (containers_.size() * 1000 * 1000) / static_cast<double>(elapsedMicro);

				std::cout << howMany << " snapshots, " << elapsedInSeconds << " seconds, " << persec << " snapshots per second" << std::endl;
			}
			*/
		};

		typedef map<string, GroupInfo> GroupMap;

		GroupMap groups_;

		GroupInfo* GetGroup(ContainerManager* containerManager, const string& groupName)
		{
			auto i = groups_.find(groupName);

			if(i == groups_.end())
				i = groups_.insert(std::move(pair<string, GroupInfo>(groupName, GroupInfo(containerManager, groupName)))).first;
			
			return &i->second;
		}

	public:
		void AddContainer(ContainerManager* containerManager, const string& groupName, shared_ptr<ITioContainer> container)
		{
			GetGroup(containerManager, groupName)->AddContainer(container);
		}

		bool RemoveContainer(const string& groupName, const string& containerName)
		{
			return false;
		}
		
		bool SubscribeGroup(ContainerManager* containerManager, const string& groupName, const shared_ptr<TioTcpSession>& session, const string& start)
		{
			GetGroup(containerManager, groupName)->Subscribe(session, start);
			return true;
		}
	};


	class BinaryProtocolLogger
	{
		map<string, unsigned> globalContainerHandle_;
		int lastGlobalHandle_;
		logdb::File f_;

		void RawLog(const string& what)
		{
			f_.Write(what.c_str(), what.size());
		}

	public:

		BinaryProtocolLogger()
			: lastGlobalHandle_(0)
			, f_(NULL)
		{
		}

		void Start(const std::string& logFilePath)
		{
			f_.Create(logFilePath.c_str());
			f_.SetPointer(f_.GetFileSize());
		}

		void SerializeTioData(string* lineLog, const TioData& data)
		{
			string serialized;

			switch(data.GetDataType())
			{
			case TioData::None:
				lineLog->append(",n,");
				break;
			case TioData::String:
				lineLog->append(",s");
				lineLog->append(lexical_cast<string>(data.GetSize()));
				lineLog->append(",");
				lineLog->append(data.AsSz(), data.GetSize());
				break;
			case TioData::Int:
				lineLog->append(",i");
				serialized = lexical_cast<string>(data.AsInt());
				lineLog->append(lexical_cast<string>(serialized.length()));
				lineLog->append(",");
				lineLog->append(serialized);
				break;
			case TioData::Double:
				lineLog->append(",d");
				serialized = lexical_cast<string>(data.AsDouble());
				lineLog->append(lexical_cast<string>(serialized.length()));
				lineLog->append(",");
				lineLog->append(serialized);
				break;
			}
		}

		void LogMessage(ITioContainer* container, PR1_MESSAGE* message)
		{
			if(!f_.IsValid())
				return;

			bool b;
			int command;

			b = Pr1MessageGetField(message, MESSAGE_FIELD_ID_COMMAND, &command);

			if(!b)
				return;

			string logLine;
			logLine.reserve(100);

			unsigned& globalHandle = globalContainerHandle_[container->GetName()];
			
			SYSTEMTIME now;
			char timeString[64];
			GetLocalTime(&now);

			_snprintf_s(
				timeString,
				sizeof(timeString),
				_TRUNCATE,
				"%04d-%02d-%02d %02d:%02d:%02d.%03d",
				now.wYear,
				now.wMonth,
				now.wDay,
				now.wHour,
				now.wMinute,
				now.wSecond,
				now.wMilliseconds);

			logLine.append(timeString);

			bool logCommand = true;

			switch(command)
			{
			case TIO_COMMAND_CREATE:
			case TIO_COMMAND_OPEN:
				if(globalHandle == 0)
				{
					string containerName = container->GetName();
					string containerType = container->GetType();
					
					globalHandle = ++lastGlobalHandle_;
					logLine.append(",create,");
					logLine.append(lexical_cast<string>(globalHandle));

					logLine.append(",s");
					logLine.append(lexical_cast<string>(containerName.size()));
					logLine.append(",");

					logLine.append(containerName);

					logLine.append(",s");
					logLine.append(lexical_cast<string>(containerType.size()));
					logLine.append(",");

					logLine.append(containerType);
					logLine.append("\n");

					RawLog(logLine);
				}

				return;

				break;
			case TIO_COMMAND_PUSH_BACK:
				logLine.append(",push_back");
				break;
			case TIO_COMMAND_PUSH_FRONT:
				logLine.append(",push_front");
				break;
			case TIO_COMMAND_POP_BACK:
				logLine.append(",pop_back");
				break;
			case TIO_COMMAND_POP_FRONT:
				logLine.append(",pop_front");
				break;
			case TIO_COMMAND_SET:
				logLine.append(",set");
				break;
			case TIO_COMMAND_INSERT:
				logLine.append(",insert");
				break;
			case TIO_COMMAND_DELETE:
				logLine.append(",delete");
				break;
			case TIO_COMMAND_CLEAR:
				logLine.append(",clear");
				break;
			case TIO_COMMAND_PROPSET:
				logLine.append(",propset");
				break;

			case TIO_COMMAND_GROUP_ADD:
				{
					string groupName, containerName;
					bool b;

					b = Pr1MessageGetField(message, MESSAGE_FIELD_ID_GROUP_NAME, &groupName);
					if(!b)
						break;

					b = Pr1MessageGetField(message, MESSAGE_FIELD_ID_CONTAINER_NAME, &containerName);
					if(!b)
						break;

					
					logLine.append(",group_add,");
					logLine.append(lexical_cast<string>(globalHandle));

					logLine.append(",s");
					logLine.append(lexical_cast<string>(groupName.size()));
					logLine.append(",");
					logLine.append(groupName);

					logLine.append(",s");
					logLine.append(lexical_cast<string>(containerName.size()));
					logLine.append(",");
					logLine.append(containerName);
					
					logLine.append("\n");

					RawLog(logLine);
					
				}

				return;
			default:
				logCommand = false;
			}

			if(!logCommand)
				return;

			int handle;
			TioData key, value, metadata;

			Pr1MessageGetHandleKeyValueAndMetadata(message, &handle, &key, &value, &metadata);

			logLine.append(",");
			logLine.append(lexical_cast<string>(globalHandle));

			SerializeTioData(&logLine, key);
			SerializeTioData(&logLine, value);
			SerializeTioData(&logLine, metadata);
			logLine.append("\n");

			RawLog(logLine);
		}
	};

	
	class TioTcpServer
	{
	public:
		typedef std::function<void (Command&, ostream&, size_t*, shared_ptr<TioTcpSession>)> CommandFunction;

		enum DiffSessionType
		{
			DiffSessionType_Map,
			DiffSessionType_List
		};

		struct DiffSessionInfo
		{
			unsigned int diffID;
			bool firstQuerySent;
			DiffSessionType diffType;
			shared_ptr<ITioContainer> source;
			shared_ptr<ITioContainer> destination;
			unsigned int subscriptionCookie;
		};


	private:

		struct MetaContainers
		{
			shared_ptr<ITioContainer> users;
			shared_ptr<ITioContainer> sessions;
			shared_ptr<ITioContainer> sessionLastCommand;
		};

		struct KeyPopperInfo
		{
			unsigned int handle;
			weak_ptr<TioTcpSession> session;
			string key;

			KeyPopperInfo()
			{
				handle = 0;
			}

			KeyPopperInfo(shared_ptr<TioTcpSession> session, unsigned int handle, const string& key)
			{
				this->session = session;
				this->handle = handle;
				this->key = key;
			}
		};

		typedef map< string /* key */, deque<KeyPopperInfo> > KeyPoppersByKey;
		typedef map< string /* full qualified name*/, KeyPoppersByKey  > KeyPoppersPerContainerMap;
		
		
		KeyPoppersPerContainerMap keyPoppersPerContainer_;
		tio::recursive_mutex keyPoppersPerContainerMutex_;


		struct NextPopperInfo
		{
			unsigned int handle;
			weak_ptr<TioTcpSession> session;

			NextPopperInfo()
			{
				handle = 0;
			}

			NextPopperInfo(shared_ptr<TioTcpSession> session, unsigned int handle)
			{
				this->session = session;
				this->handle = handle;
			}
		};
		
		// map<diff handle, DiffSessionInfo >
		typedef map< unsigned int, DiffSessionInfo > DiffSessions;
		DiffSessions diffSessions_;
		tio::recursive_mutex diffSessionsMutex_;

		unsigned int lastSessionID_;
		unsigned int lastQueryID_;
		unsigned int lastDiffID_;

		typedef map< string, deque<NextPopperInfo> > NextPoppersMap;
		NextPoppersMap nextPoppers_;
		tio::recursive_mutex nextPoppersMutex_;

		typedef void (tio::TioTcpServer::* CommandCallbackFunction)(tio::Command &,std::ostream &,size_t *,std::shared_ptr<TioTcpSession>);

		typedef std::map<string, CommandCallbackFunction> CommandFunctionMap;
		CommandFunctionMap dispatchMap_;

		Auth auth_;

		bool serverPaused_;
		
		tcp::acceptor acceptor_;
		asio::io_service& io_service_;
		
		typedef std::set< shared_ptr<TioTcpSession> > SessionsSet;
		SessionsSet sessions_;
		tio::recursive_mutex sessionsMutex_;

		ContainerManager& containerManager_;

		MetaContainers metaContainers_;

		BinaryProtocolLogger logger_;

		GroupManager groupManager_;
				
		void DoAccept();
		void OnAccept(shared_ptr<TioTcpSession> client, const error_code& err);
		
		pair<shared_ptr<ITioContainer>, int> GetRecordBySpec(const string& spec, shared_ptr<TioTcpSession> session);

		size_t ParseDataCommand(Command& cmd, string* containerType, string* containerName, shared_ptr<ITioContainer>* container, 
			TioData* key, TioData* value, TioData* metadata, shared_ptr<TioTcpSession> session, unsigned int* handle = NULL);

		void LoadDispatchMap();

		void SendResultSet(shared_ptr<TioTcpSession> session, shared_ptr<ITioResultSet> resultSet);

		void OnCommand_Query(Command& cmd, ostream& answer, size_t* moreDataSize, shared_ptr<TioTcpSession> session);

		void OnCommand_QueryEx(Command& cmd, ostream& answer, size_t* moreDataSize, shared_ptr<TioTcpSession> session);

		void OnCommand_Diff_Start(Command& cmd, ostream& answer, size_t* moreDataSize, shared_ptr<TioTcpSession> session);
		void OnCommand_Diff(Command& cmd, ostream& answer, size_t* moreDataSize, shared_ptr<TioTcpSession> session);

		void OnCommand_GroupAdd(Command& cmd, ostream& answer, size_t* moreDataSize, shared_ptr<TioTcpSession> session);
		void OnCommand_GroupSubscribe(Command& cmd, ostream& answer, size_t* moreDataSize, shared_ptr<TioTcpSession> session);

		void OnCommand_Ping(Command& cmd, ostream& answer, size_t* moreDataSize, shared_ptr<TioTcpSession> session);
		void OnCommand_Version(Command& cmd, ostream& answer, size_t* moreDataSize, shared_ptr<TioTcpSession> session);
		
		void OnCommand_CreateContainer_OpenContainer(Command& cmd, ostream& answer, size_t* moreDataSize, shared_ptr<TioTcpSession> session);
		void OnCommand_CloseContainer(Command& cmd, ostream& answer, size_t* moreDataSize, shared_ptr<TioTcpSession> session);

		void OnCommand_DeleteContainer(Command& cmd, ostream& answer, size_t* moreDataSize, shared_ptr<TioTcpSession> session);

		void OnCommand_ListHandles(Command& cmd, ostream& answer, size_t* moreDataSize, shared_ptr<TioTcpSession> session);
		

		void OnCommand_Pop(Command& cmd, ostream& answer, size_t* moreDataSize, shared_ptr<TioTcpSession> session);
		
		void OnCommand_GetRecordCount(Command& cmd, ostream& answer, size_t* moreDataSize, shared_ptr<TioTcpSession> session);

		void OnCommand_SubscribeUnsubscribe(Command& cmd, ostream& answer, size_t* moreDataSize, shared_ptr<TioTcpSession> session);

		void OnCommand_WnpNext(Command& cmd, ostream& answer, size_t* moreDataSize, shared_ptr<TioTcpSession> session);
		void OnCommand_WnpKey(Command& cmd, ostream& answer, size_t* moreDataSize, shared_ptr<TioTcpSession> session);

		string GetFullQualifiedName(shared_ptr<ITioContainer> container);

		void OnCommand_PauseResume(Command& cmd, ostream& answer, size_t* moreDataSize, shared_ptr<TioTcpSession> session);

		void OnCommand_Auth(Command& cmd, ostream& answer, size_t* moreDataSize, shared_ptr<TioTcpSession> session);
		void OnCommand_SetPermission(Command& cmd, ostream& answer, size_t* moreDataSize, shared_ptr<TioTcpSession> session);

		void OnAnyDataCommand(Command& cmd, ostream& answer, size_t* moreDataSize, shared_ptr<TioTcpSession> session);
		
		void OnModify(Command& cmd, ostream& answer, size_t* moreDataSize, shared_ptr<TioTcpSession> session);

		void OnCommand_CustomCommand(Command& cmd, ostream& answer, size_t* moreDataSize, shared_ptr<TioTcpSession> session);
		void OnCommand_Clear(Command& cmd, ostream& answer, size_t* moreDataSize, shared_ptr<TioTcpSession> session);

		void RemoveClient(shared_ptr<TioTcpSession> client);
		bool CheckCommandAccess(const string& command, ostream& answer, shared_ptr<TioTcpSession> session);
		bool CheckObjectAccess(const string& objectType, const string& objectName, const string& command, ostream& answer, shared_ptr<TioTcpSession> session);

		string GetConfigValue(const string& key);

		void InitializeMetaContainers();

		shared_ptr<ITioContainer> GetContainerAndParametersFromRequest(const PR1_MESSAGE* message, shared_ptr<TioTcpSession> session, TioData* key, TioData* value, TioData* metadata);

		unsigned int GenerateSessionId();
		unsigned int GenerateDiffId();

		void HandlePushBackWaitAndPop( shared_ptr<ITioContainer> container, const TioData& key, const TioData& value, const TioData& metadata);

		void HandleKeyValueWaitAndPop(shared_ptr<ITioContainer> container, 
			const TioData& key, const TioData& value, const TioData& metadata);

	public:
		TioTcpServer(ContainerManager& containerManager,asio::io_service& io_service, const tcp::endpoint& endpoint, const std::string& logFilePath);
		void OnClientFailed(shared_ptr<TioTcpSession> client, const error_code& err);
		void OnCommand(Command& cmd, ostream& answer, size_t* moreDataSize, shared_ptr<TioTcpSession> session);

		void PostCallback(function<void()> callback);
		
		void OnBinaryCommand(shared_ptr<TioTcpSession> session, PR1_MESSAGE* message);

		void Start();

		Auth& GetAuth();
		unsigned CreateNewQueryId();
	};

	void StartServer();
}



