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
	
	using boost::shared_ptr;
	using boost::system::error_code;

	using boost::lexical_cast;
	using boost::bad_lexical_cast;

	using boost::split;
	using boost::is_any_of;

	using boost::tuple;

	using std::setfill;
	using std::setw;
	using std::hex;

	namespace asio = boost::asio;
	using namespace boost::asio::ip;

	enum ParameterCountCheckType
	{
		exact,
		at_least
	};

	bool CheckParameterCount(Command& cmd, size_t requiredParameters, ParameterCountCheckType type = exact)
	{
		if(type == exact && cmd.GetParameters().size() == requiredParameters)
			return true;
		else if(type == at_least && cmd.GetParameters().size()>= requiredParameters)
			return true;

		return false;
	}


	void TioTcpServer::OnClientFailed(shared_ptr<TioTcpSession> client, const error_code& err)
	{
		//
		// we'll post, so we'll not invalidate the client now
		// it will probably be called from a TioTcpSession callback
		// and removing now will delete the session's pointer
		//
		io_service_.post(boost::bind(&TioTcpServer::RemoveClient, this, client));
	}

	void TioTcpServer::RemoveClient(shared_ptr<TioTcpSession> client)
	{
		SessionsSet::iterator i = sessions_.find(client);

		//
		// it can happen if we receive more than one error notification
		// for this connection
		//
		if(i == sessions_.end())
			return; // something is VERY wrong...

#ifdef _DEBUG
		std::cout << "disconnect" << std::endl;
#endif
		metaContainers_.sessions->Delete(lexical_cast<string>(client->GetID()), TIONULL, TIONULL);
		sessions_.erase(i);

#if 0
		//
		// remove all 'wait and pop next' references to this session
		//
		for(PoppersMap::iterator i = nextPoppers_.begin() ; i != nextPoppers_.end() ; ++i)
		{
			deque<PopperInfo> q = i->second;
			size_t x = 0;

			while(x != q.size())
			{
				if(q[x].session == client.get())
					q.erase(q.begin() + x);
				else
					++x;
			}
		}
#endif
	}


	TioTcpServer::TioTcpServer(ContainerManager& containerManager, asio::io_service& io_service, const tcp::endpoint& endpoint) :
		containerManager_(containerManager),
		acceptor_(io_service, endpoint),
		io_service_(io_service),
		lastSessionID_(0),
		lastQueryID_(0)
	{
		LoadDispatchMap();
		InitializeMetaContainers();
	}

	void TioTcpServer::InitializeMetaContainers()
	{
		//
		// users
		//
		metaContainers_.users = containerManager_.CreateContainer("volatile_map", "meta/users");

		try
		{
			string schema = metaContainers_.users->GetProperty("schema");
		}
		catch(std::exception&)
		{
			metaContainers_.users->SetProperty("schema", "password type|password");
		}

		string userContainerType = containerManager_.ResolveAlias("users");

		auth_.SetObjectDefaultRule(userContainerType, "meta/users", Auth::deny);
		auth_.AddObjectRule(userContainerType, "meta/users", "*", "__admin__", Auth::allow);

		//
		// sessions
		//
		metaContainers_.sessions = containerManager_.CreateContainer("volatile_map", "meta/sessions");

		metaContainers_.sessionLastCommand = containerManager_.CreateContainer("volatile_map", "meta/session_last_command");


	}

	Auth& TioTcpServer::GetAuth()
	{
		return auth_;
	}
	
	void TioTcpServer::DoAccept()
	{
		shared_ptr<TioTcpSession> session(new TioTcpSession(io_service_, *this, ++lastSessionID_));

		acceptor_.async_accept(session->GetSocket(),
			boost::bind(&TioTcpServer::OnAccept, this, session, asio::placeholders::error));
	}

	void TioTcpServer::OnAccept(shared_ptr<TioTcpSession> session, const error_code& err)
	{
		if(!!err)
		{
			throw err;
		}

		sessions_.insert(session);

		metaContainers_.sessions->Insert(lexical_cast<string>(session->GetID()), TIONULL, TIONULL);

		session->OnAccept();

		DoAccept();
	}

	shared_ptr<ITioContainer> TioTcpServer::GetContainerAndParametersFromRequest(const PR1_MESSAGE* message, shared_ptr<TioTcpSession> session, TioData* key, TioData* value, TioData* metadata)
	{
		int handle;

		Pr1MessageGetHandleKeyValueAndMetadata(message, &handle, key, value, metadata);

		if(!handle)
			throw std::runtime_error("");

		return session->GetRegisteredContainer(handle);
	}

	

	void TioTcpServer::OnBinaryCommand(shared_ptr<TioTcpSession> session, PR1_MESSAGE* message)
	{
		bool b;
		int command;

		pr1_message_parse(message);

		b = Pr1MessageGetField(message, MESSAGE_FIELD_ID_COMMAND, &command);

		if(!b)
		{
			session->SendBinaryErrorAnswer(TIO_ERROR_MISSING_PARAMETER);
			return;
		}

		try
		{
			switch(command)
			{
			case TIO_COMMAND_PING:
				{
					string payload;
					b = Pr1MessageGetField(message, MESSAGE_FIELD_ID_VALUE, &payload);

					if(!b)
					{
						session->SendBinaryErrorAnswer(TIO_ERROR_MISSING_PARAMETER);
						break;
					}

					shared_ptr<PR1_MESSAGE> answer = Pr1CreateMessage();

					pr1_message_add_field_int(answer.get(), MESSAGE_FIELD_ID_COMMAND, TIO_COMMAND_ANSWER);
					pr1_message_add_field_string(answer.get(), MESSAGE_FIELD_ID_VALUE, payload.c_str());


					session->SendBinaryMessage(answer);
				}
				break;
			case TIO_COMMAND_OPEN:
			case TIO_COMMAND_CREATE:
				{
					string name, type;
					b = Pr1MessageGetField(message, MESSAGE_FIELD_ID_NAME, &name);

					if(!b)
					{
						session->SendBinaryErrorAnswer(TIO_ERROR_MISSING_PARAMETER);
						break;
					}

					Pr1MessageGetField(message, MESSAGE_FIELD_ID_TYPE, &type);

					shared_ptr<ITioContainer> container;

					if(command == TIO_COMMAND_CREATE)
					{
						if(type.empty())
						{
							session->SendBinaryErrorAnswer(TIO_ERROR_MISSING_PARAMETER);
							break;
						}

						container = containerManager_.CreateContainer(type, name);
					}
					else if(command == TIO_COMMAND_OPEN)
					{
						container = containerManager_.OpenContainer(type, name);
					}

					unsigned int handle = session->RegisterContainer(name, container);

					shared_ptr<PR1_MESSAGE> answer = Pr1CreateMessage();

					pr1_message_add_field_int(answer.get(), MESSAGE_FIELD_ID_COMMAND, TIO_COMMAND_ANSWER);
					pr1_message_add_field_int(answer.get(), MESSAGE_FIELD_ID_HANDLE, handle);
				
					session->SendBinaryMessage(answer);
				}
				break;
				case TIO_COMMAND_CLOSE:
				{
					bool b;
					int handle;
					string start;
					b = Pr1MessageGetField(message, MESSAGE_FIELD_ID_HANDLE, &handle);

					if(!b)
					{
						session->SendBinaryErrorAnswer(TIO_ERROR_MISSING_PARAMETER);
						break;
					}

					session->CloseContainerHandle(handle);

					session->SendBinaryAnswer();
				}
				break;

				case TIO_COMMAND_GET:
				case TIO_COMMAND_PROPGET:
				{
					TioData searchKey;

					shared_ptr<ITioContainer> container = GetContainerAndParametersFromRequest(message, session, &searchKey, NULL, NULL);

					TioData key, value, metadata;

					if(command == TIO_COMMAND_GET)
						container->GetRecord(searchKey, &key, &value, &metadata);
					else if(command == TIO_COMMAND_PROPGET)
					{
						value = container->GetProperty(searchKey.AsSz());
					}
					else
						throw std::runtime_error("INTERNAL ERROR");


					session->SendBinaryAnswer(&key, &value, &metadata);
				}
				break;

				case TIO_COMMAND_POP_FRONT:
				case TIO_COMMAND_POP_BACK:
				{
					shared_ptr<ITioContainer> container = GetContainerAndParametersFromRequest(message, session, NULL, NULL, NULL);

					TioData key, value, metadata;

					if(command == TIO_COMMAND_POP_BACK)
						container->PopBack(&key, &value, &metadata);
					else if(command == TIO_COMMAND_POP_FRONT)
						container->PopFront(&key, &value, &metadata);
					else
						throw std::runtime_error("INTERNAL ERROR");

					session->SendBinaryAnswer(&key, &value, &metadata);
				}
				break;

				case TIO_COMMAND_PUSH_BACK:
				case TIO_COMMAND_PUSH_FRONT:
				case TIO_COMMAND_SET:
				case TIO_COMMAND_INSERT:
				case TIO_COMMAND_DELETE:
				case TIO_COMMAND_CLEAR:
				case TIO_COMMAND_PROPSET:
				{
					TioData key, value, metadata;

					shared_ptr<ITioContainer> container = GetContainerAndParametersFromRequest(message, session, &key, &value, &metadata);

					if(command == TIO_COMMAND_PUSH_BACK)
						container->PushBack(key, value, metadata);
					else if(command == TIO_COMMAND_PUSH_FRONT)
						container->PushFront(key, value, metadata);
					else if(command == TIO_COMMAND_SET)
						container->Set(key, value, metadata);
					else if(command == TIO_COMMAND_INSERT)
						container->Insert(key, value, metadata);
					else if(command == TIO_COMMAND_DELETE)
						container->Delete(key, value, metadata);
					else if(command == TIO_COMMAND_CLEAR)
						container->Clear();
					else if(command == TIO_COMMAND_PROPSET)
					{
						if(key.GetDataType() != TioData::Sz	|| value.GetDataType() != TioData::Sz)
							throw std::runtime_error("properties key and value should be strings");

						container->SetProperty(key.AsSz(), value.AsSz());
					}
					else
						throw std::runtime_error("INTERNAL ERROR");

					session->SendBinaryAnswer();
				}
				break;

				case TIO_COMMAND_COUNT:
				{
					shared_ptr<ITioContainer> container = GetContainerAndParametersFromRequest(message, session, NULL, NULL, NULL);

					int count = container->GetRecordCount();

					shared_ptr<PR1_MESSAGE> answer = Pr1CreateAnswerMessage(NULL, NULL, NULL);
					pr1_message_add_field_int(answer.get(), MESSAGE_FIELD_ID_VALUE, count);

					session->SendBinaryMessage(answer);
				}
				break;

				case TIO_COMMAND_QUERY:
				{
					int start, end;

					shared_ptr<ITioContainer> container = GetContainerAndParametersFromRequest(message, session, NULL, NULL, NULL);
					
					if(!Pr1MessageGetField(message, MESSAGE_FIELD_ID_START, &start))
						start = 0;

					if(!Pr1MessageGetField(message, MESSAGE_FIELD_ID_END, &end))
						end = 0;

					shared_ptr<ITioResultSet> resultSet = container->Query(start, end, TIONULL);

					//
					// TODO: hardcoded query id
					//
					session->SendBinaryResultSet(resultSet, 1);
					
				}
				break;

				case TIO_COMMAND_SUBSCRIBE:
				{
					bool b;
					int handle;
					string start_string;
					int start_int;
					b = Pr1MessageGetField(message, MESSAGE_FIELD_ID_HANDLE, &handle);

					if(!b)
					{
						session->SendBinaryErrorAnswer(TIO_ERROR_MISSING_PARAMETER);
						break;
					}

					b = Pr1MessageGetField(message, MESSAGE_FIELD_ID_KEY, &start_string);
					if(!b)
						if(Pr1MessageGetField(message, MESSAGE_FIELD_ID_KEY, &start_int))
							start_string = lexical_cast<string>(start_int);

					session->BinarySubscribe(handle, start_string);
				}
				break;

				case TIO_COMMAND_UNSUBSCRIBE:
				{
					bool b;
					int handle;
					b = Pr1MessageGetField(message, MESSAGE_FIELD_ID_HANDLE, &handle);

					if(!b)
					{
						session->SendBinaryErrorAnswer(TIO_ERROR_MISSING_PARAMETER);
						break;
					}

					session->Unsubscribe(handle);

					session->SendBinaryAnswer();
				}
				break;

			default:
				session->SendBinaryErrorAnswer(TIO_ERROR_PROTOCOL);
				return;
			}
		} 
		catch(std::exception& ex)
		{
			ex;
			session->SendBinaryErrorAnswer(TIO_ERROR_PROTOCOL);
		}
	}


	void TioTcpServer::OnCommand(Command& cmd, ostream& answer, size_t* moreDataSize, shared_ptr<TioTcpSession> session)
	{
		if(cmd.GetCommand() == "stop")
		{
			io_service_.stop();
			return;
		}

		CommandFunctionMap::iterator i = dispatchMap_.find(cmd.GetCommand());

		if(i != dispatchMap_.end())
		{
			CommandFunction& f = i->second;

			try
			{
				f(cmd, answer, moreDataSize, session);
			}
			catch(std::exception& ex)
			{
				BOOST_ASSERT(false && "handler functions not supposed to throw exceptions");
				MakeAnswer(error, answer, string("internal error: ") + ex.what());
			}
		}
		else			
		{
			MakeAnswer(error, answer, "invalid command");
		}

		if(*moreDataSize == 0)
		{
			metaContainers_.sessionLastCommand->Set(
				lexical_cast<string>(session->GetID()),
				TioData(cmd.GetSource().c_str(), false),
				cmd.GetDataBuffer()->GetSize() ? 
					TioData(cmd.GetDataBuffer()->GetRawBuffer(), cmd.GetDataBuffer()->GetSize()) :
					TIONULL);
		}
	}

	//
	// commands
	//
	void TioTcpServer::LoadDispatchMap()
	{
		dispatchMap_["ping"] = boost::bind(&TioTcpServer::OnCommand_Ping, this, _1, _2, _3, _4);
		dispatchMap_["ver"] = boost::bind(&TioTcpServer::OnCommand_Version, this, _1, _2, _3, _4);
		
		dispatchMap_["create"] = boost::bind(&TioTcpServer::OnCommand_CreateContainer_OpenContainer, this, _1, _2, _3, _4);
		dispatchMap_["open"] = boost::bind(&TioTcpServer::OnCommand_CreateContainer_OpenContainer, this, _1, _2, _3, _4);
		dispatchMap_["close"] = boost::bind(&TioTcpServer::OnCommand_CloseContainer, this, _1, _2, _3, _4);

		dispatchMap_["delete_container"] = boost::bind(&TioTcpServer::OnCommand_DeleteContainer, this, _1, _2, _3, _4);

		dispatchMap_["list_handles"] = boost::bind(&TioTcpServer::OnCommand_ListHandles, this, _1, _2, _3, _4);
		
		dispatchMap_["push_back"] = boost::bind(&TioTcpServer::OnAnyDataCommand, this, _1, _2, _3, _4);
		dispatchMap_["push_front"] = boost::bind(&TioTcpServer::OnAnyDataCommand, this, _1, _2, _3, _4);
		
		dispatchMap_["pop_back"] = boost::bind(&TioTcpServer::OnCommand_Pop, this, _1, _2, _3, _4);
		dispatchMap_["pop_front"] = boost::bind(&TioTcpServer::OnCommand_Pop, this, _1, _2, _3, _4);

		dispatchMap_["modify"] = boost::bind(&TioTcpServer::OnModify, this, _1, _2, _3, _4);

		dispatchMap_["wnp_next"] = boost::bind(&TioTcpServer::OnCommand_WnpNext, this, _1, _2, _3, _4);
		dispatchMap_["wnp_key"] = boost::bind(&TioTcpServer::OnCommand_WnpKey, this, _1, _2, _3, _4);
		
		dispatchMap_["set"] = boost::bind(&TioTcpServer::OnAnyDataCommand, this, _1, _2, _3, _4);
		dispatchMap_["insert"] = boost::bind(&TioTcpServer::OnAnyDataCommand, this, _1, _2, _3, _4);
		dispatchMap_["delete"] = boost::bind(&TioTcpServer::OnAnyDataCommand, this, _1, _2, _3, _4);
		dispatchMap_["clear"] = boost::bind(&TioTcpServer::OnCommand_Clear, this, _1, _2, _3, _4);

		dispatchMap_["get_property"] = boost::bind(&TioTcpServer::OnAnyDataCommand, this, _1, _2, _3, _4);
		dispatchMap_["set_property"] = boost::bind(&TioTcpServer::OnAnyDataCommand, this, _1, _2, _3, _4);

		dispatchMap_["get"] = boost::bind(&TioTcpServer::OnAnyDataCommand, this, _1, _2, _3, _4);

		dispatchMap_["get_count"] = boost::bind(&TioTcpServer::OnCommand_GetRecordCount, this, _1, _2, _3, _4);

		dispatchMap_["subscribe"] = boost::bind(&TioTcpServer::OnCommand_SubscribeUnsubscribe, this, _1, _2, _3, _4);
		dispatchMap_["unsubscribe"] = boost::bind(&TioTcpServer::OnCommand_SubscribeUnsubscribe, this, _1, _2, _3, _4);

		dispatchMap_["command"] = boost::bind(&TioTcpServer::OnCommand_CustomCommand, this, _1, _2, _3, _4);
		
		dispatchMap_["auth"] = boost::bind(&TioTcpServer::OnCommand_Auth, this, _1, _2, _3, _4);

		dispatchMap_["set_permission"] = boost::bind(&TioTcpServer::OnCommand_SetPermission, this, _1, _2, _3, _4);

		dispatchMap_["query"] = boost::bind(&TioTcpServer::OnCommand_Query, this, _1, _2, _3, _4);
		
		dispatchMap_["diff_start"] = boost::bind(&TioTcpServer::OnCommand_Diff_Start, this, _1, _2, _3, _4);
		dispatchMap_["diff"] = boost::bind(&TioTcpServer::OnCommand_Diff, this, _1, _2, _3, _4);

	}

	std::string Serialize(const std::list<const TioData*>& fields)
	{
		stringstream header, values;

		// message identification
		header << "X1";
		
		// field count
		header << setfill('0') << setw(4) << hex << fields.size() << "C";

		BOOST_FOREACH(const TioData* field, fields)
		{
			unsigned int currentStreamSize = values.str().size();
			unsigned int size = 0;
			char dataTypeCode = 'X';

			if(field && field->GetDataType() != TioData::None)
			{	
				switch(field->GetDataType())
				{
				case TioData::Sz:
					values << field->AsSz();
					dataTypeCode = 'S';
					break;
				case TioData::Int:
					values << field->AsInt();
					dataTypeCode = 'I';
					break;
				case TioData::Double:
					values << field->AsDouble();
					dataTypeCode = 'D';
					break;
				}

				size = values.str().size() - currentStreamSize;

				values << " ";
			}
			else
			{
				size = 0;
			}

			header << setfill('0') << setw(4) << hex << size << dataTypeCode;
		}

		header << " " << values.str();

		return header.str();
	}


	void TioTcpServer::OnCommand_Query(Command& cmd, ostream& answer, size_t* moreDataSize, shared_ptr<TioTcpSession> session)
	{
		if(!CheckParameterCount(cmd, 1, at_least))
		{
			MakeAnswer(error, answer, "invalid parameter count");
			return;
		}

		shared_ptr<ITioContainer> container;
		unsigned int handle;

		try
		{
			handle = lexical_cast<unsigned int>(cmd.GetParameters()[0]);
			container = session->GetRegisteredContainer(handle);
		}
		catch(std::exception&)
		{
			MakeAnswer(error, answer, "invalid handle");
			return;
		}

		int start = 0, end = 0;
		TioData query;

		try
		{
			if(cmd.GetParameters().size() > 1)
				start = lexical_cast<int>(cmd.GetParameters()[1]);
			
			if(cmd.GetParameters().size() > 2)
				end = lexical_cast<int>(cmd.GetParameters()[2]);
		}
		catch(bad_lexical_cast&)
		{
			MakeAnswer(error, answer, "invalid parameter");
			return;
		}

		try
		{
			SendResultSet(session, container->Query(start, end, query));
		}
		catch(std::exception& ex)
		{
			MakeAnswer(error, answer, ex.what());
			return;
		}
	}

	void MapChangeRecorder(const TioTcpServer::DiffSessionInfo& info,
		const string& event_name, const TioData& key, const TioData& value, const TioData& metadata)
	{		
		if(event_name == "set" || event_name == "insert")
		{
			std::list<const TioData*> fields;
			TioData eventString(event_name);

			fields.push_back(value.GetDataType() != TioData::None ? &value : NULL);
			fields.push_back(metadata.GetDataType() != TioData::None ?& metadata : NULL);

			string serialized = Serialize(fields);

			info.destination->Set(key, serialized, event_name);
		}
		if(event_name == "delete")
			info.destination->Delete(key, TIONULL, event_name);
		else if(event_name == "clear")
		{
			//
			// Looks ugly, but there's no other (easy) way to do this. On clear
			// event, there's already no record on the source container, so we
			// can't just enumerate the record and generate a delete event for all 
			// of them. Since clients can't create key starting with "__", no
			// conflict will happen
			//
			info.destination->Clear();
			info.destination->Set("__special__", "clear", TIONULL);
		}

		//
		// TODO: support other events
		//
	}

	void ListChangeRecorder(const TioTcpServer::DiffSessionInfo& info,
		const string& event_name, const TioData& key, const TioData& value, const TioData& metadata)
	{
		std::list<const TioData*> fields;
		TioData eventString(event_name);

		fields.push_back(&eventString);
		fields.push_back(key.GetDataType() != TioData::None ? &key : NULL);
		fields.push_back(value.GetDataType() != TioData::None ? &value : NULL);
		fields.push_back(metadata.GetDataType() != TioData::None ?& metadata : NULL);

		string serialized = Serialize(fields);

		info.destination->PushBack(TIONULL, serialized, info.source->GetName());
	}

	void TioTcpServer::OnCommand_Diff_Start(Command& cmd, ostream& answer, size_t* moreDataSize, shared_ptr<TioTcpSession> session)
	{
		if(!CheckParameterCount(cmd, 1, exact))
		{
			MakeAnswer(error, answer, "invalid parameter count");
			return;
		}

		shared_ptr<ITioContainer> container;
		unsigned int handle;

		try
		{
			handle = lexical_cast<unsigned int>(cmd.GetParameters()[0]);
			container = session->GetRegisteredContainer(handle);
		}
		catch(std::exception&)
		{
			MakeAnswer(error, answer, "invalid handle");
			return;
		}

		try
		{
			stringstream destinationName;
			shared_ptr<ITioContainer> diffContainer;
			DiffSessionType diffType = DiffSessionType_Map;
			string diffHandleType;

			string containerType = container->GetType();
			string destinationContainerType;

			if(IsMapContainer(container))
			{
				diffType = DiffSessionType_Map;
				diffHandleType = "diff_map";
				destinationContainerType = "volatile_map";
			}
				
			else if(IsListContainer(container))
			{
				diffType = DiffSessionType_List;
				diffHandleType = "diff_list";
				destinationContainerType = "volatile_list";
			}
			else
			{
				MakeAnswer(error, answer, "cannot create diff for this container type");
			}

			//
			// generating a unique name for destination container
			//
			unsigned int diffID = ++lastDiffID_;
			destinationName << "__/diff/" << container->GetName() << "/" << diffID;
			
			diffContainer = containerManager_.CreateContainer(destinationContainerType, destinationName.str());

			DiffSessionInfo info;
			info.firstQuerySent = false;
			info.diffID = diffID;
			info.source = container;
			info.destination = diffContainer;
			info.diffType = diffType;

			diffSessions_[info.diffID] = info;

			MakeAnswer(success, answer, diffHandleType, lexical_cast<string>(info.diffID));
		}
		catch(std::exception& ex)
		{
			MakeAnswer(error, answer, ex.what());
			return;
		}
	}

	void TioTcpServer::SendResultSet(shared_ptr<TioTcpSession> session, shared_ptr<ITioResultSet> resultSet)
	{
		session->SendResultSet(resultSet, ++lastQueryID_);
	}

	//
	// The record command asks the server to record all events from a container is another container
	// It will create a kind of transaction log. Even if the client disconnects, the events will continue to
	// be recorded. It's for clients that can't loose events even in case of disconnection and
	// clients that want to receive events but can stay connected (subscription by polling)
	//
	void TioTcpServer::OnCommand_Diff(Command& cmd, ostream& answer, size_t* moreDataSize, shared_ptr<TioTcpSession> session)
	{
		if(!CheckParameterCount(cmd, 1, exact))
		{
			MakeAnswer(error, answer, "invalid parameter count");
			return;
		}

		unsigned int diffID = 0;

		try
		{
			diffID = lexical_cast<unsigned int>(cmd.GetParameters()[0]);
		}
		catch(std::exception&)
		{
			MakeAnswer(error, answer, "invalid diff handle");
			return;
		}

		DiffSessions::iterator i = diffSessions_.find(diffID);

		if(i == diffSessions_.end())
		{
			MakeAnswer(error, answer, "invalid diff handle");
			return;
		}

		DiffSessionInfo& info = i->second;

		//
		// if it's the first query, will send the entire content of container
		// and setup the incremental recording
		//

		if(!info.firstQuerySent)
		{
			if(info.diffType == DiffSessionType_List)
			{
				info.subscriptionCookie = 
					info.source->Subscribe(boost::bind(&ListChangeRecorder, info, _1, _2, _3, _4), "0");
			}
			else if(info.diffType == DiffSessionType_Map)
			{
				info.subscriptionCookie = 
					info.source->Subscribe(boost::bind(&MapChangeRecorder, info, _1, _2, _3, _4), "");
			}

			SendResultSet(session, info.destination->Query(0, 0, TIONULL));

			info.firstQuerySent = true;
		}
		else
		{
			SendResultSet(session, info.destination->Query(0, 0, TIONULL));
			info.destination->Clear();
		}

		/*
		shared_ptr<ITioContainer> source, destination;
		unsigned int sourceHandle, destinationHandle;

		try
		{
			sourceHandle = lexical_cast<unsigned int>(cmd.GetParameters()[0]);
			destinationHandle = lexical_cast<unsigned int>(cmd.GetParameters()[1]);

			source = session->GetRegisteredContainer(sourceHandle);
			destination = session->GetRegisteredContainer(destinationHandle);
		}
		catch(std::exception&)
		{
			MakeAnswer(error, answer, "invalid handle");
			return;
		}

		RecordingSessions::const_iterator i;
		
		//
		// check if it's already being recorded
		//
		i = recordingSessions_.find(source->GetName());

		if(i != recordingSessions_.end() &&
		   i->second.find(destination->GetName()) != i->second.end())
		{
			MakeAnswer(error, answer, "already recording to this container");
			return;
		}
		
		//
		// check if it will not create a loop
		//
		i = recordingSessions_.find(destination->GetName());

		if(i != recordingSessions_.end() &&
			i->second.find(source->GetName()) != i->second.end())
		{
			MakeAnswer(error, answer, 
				"container informed as destination is being recorded to the source container, it will create a loop");
			return;
		}

		//
		// TODO: write recordings to a metacontainer too
		//

		recordingSessions_[source->GetName()].insert(destination->GetName());

		//
		// TODO: should check other ways to create loops
		//
		source->Subscribe(boost::bind(&RecordChangeRecorder, source, destination, _1, _2, _3, _4), string());

		MakeAnswer(success, answer);

		*/
	}

	void TioTcpServer::OnCommand_WnpNext(Command& cmd, ostream& answer, size_t* moreDataSize, shared_ptr<TioTcpSession> session)
	{
		if(!CheckParameterCount(cmd, 1, exact))
		{
			MakeAnswer(error, answer, "invalid parameter count");
			return;
		}

		shared_ptr<ITioContainer> container;
		unsigned int handle;

		try
		{
			handle = lexical_cast<unsigned int>(cmd.GetParameters()[0]);
			container = session->GetRegisteredContainer(handle);
		}
		catch(std::exception&)
		{
			MakeAnswer(error, answer, "invalid handle");
			return;
		}

		deque<NextPopperInfo>& q = nextPoppers_[GetFullQualifiedName(container)];

		//
		// he's the only one popping and there's a record to pop,
		// the best scenario
		//
		if(q.size() == 0 && container->GetRecordCount() > 0)
		{
			TioData key, value, metadata;
			
			container->PopFront(&key, &value, &metadata);

			//
			// send answer before the event
			//
			MakeAnswer(success, answer);

			MakeEventAnswer("wnp_next", handle, key, value, metadata, answer);
		}
		else
		{
			//
			// if there are other poppers (or no data), will go to the queue
			//
			q.push_back(NextPopperInfo(session, handle));
			MakeAnswer(success, answer);
		}

	}


	void TioTcpServer::OnCommand_WnpKey(Command& cmd, ostream& answer, size_t* moreDataSize, shared_ptr<TioTcpSession> session)
	{
		try
		{
		
			string containerName, containerType;
			shared_ptr<ITioContainer> container;
			TioData key, value, metadata;
			unsigned int handle;

			size_t dataSize = ParseDataCommand(
				cmd,
				&containerType,
				&containerName,
				&container,
				&key,
				NULL,
				NULL, 
				session, 
				&handle);

			if(dataSize != 0)
			{
				*moreDataSize = dataSize;
				return;
			}

			if(key.GetDataType() != TioData::Sz)
			{
				MakeAnswer(error, answer, "invalid data type, must be string");
				return;
			}

			string fullQualifiedName = GetFullQualifiedName(container);

			KeyPoppersPerContainerMap::iterator i = keyPoppersPerContainer_.find(fullQualifiedName);

			//
			// any popper waiting?
			//
			bool popNow = false;
			
			if(i == keyPoppersPerContainer_.end() || i->second.size() == 0)
			{
				//
				// no... let's see if the wanted key is here
				//
				
				try
				{
					container->GetRecord(key, NULL, &value, &metadata);
					popNow = true;
				}
				catch (std::invalid_argument)
				{
					popNow = false;			
				}
			}
				
			if(popNow)	
			{
				//
				// send answer before the event
				//
				MakeAnswer(success, answer);

				container->Delete(key);

				MakeEventAnswer("wnp_key", handle, key, value, metadata, answer);
			}
			else
			{
				//
				// there are other pops waiting. He'll need to stay in the queue and pray
				//
				string keyAsString = key.AsSz();
				keyPoppersPerContainer_[fullQualifiedName][keyAsString].push_back(KeyPopperInfo(session, handle, keyAsString));

				MakeAnswer(success, answer);
			}
		}
		catch (std::exception& e)
		{
			MakeAnswer(error, answer, e.what());
			return;
		}
	}

	void TioTcpServer::OnCommand_CustomCommand(Command& cmd, ostream& answer, size_t* moreDataSize, shared_ptr<TioTcpSession> session)
	{
		if(!CheckParameterCount(cmd, 2, at_least))
		{
			MakeAnswer(error, answer, "invalid parameter count");
			return;
		}

		shared_ptr<ITioContainer> container;

		try
		{
			unsigned int handle;

			handle = lexical_cast<unsigned int>(cmd.GetParameters()[0]);
			container = session->GetRegisteredContainer(handle);
		}
		catch(std::exception&)
		{
			MakeAnswer(error, answer, "invalid handle");
			return;
		}

		string command;
		
		for(Command::Parameters::const_iterator i = cmd.GetParameters().begin() + 1 ; i != cmd.GetParameters().end() ; ++i)
			command += *i + " ";

		command.erase(command.end() - 1);

		string result = container->Command(command);

		MakeAnswer(success, answer, result);

	}


	void TioTcpServer::OnCommand_Clear(Command& cmd, ostream& answer, size_t* moreDataSize, shared_ptr<TioTcpSession> session)
	{
		if(!CheckParameterCount(cmd, 1, exact))
		{
			MakeAnswer(error, answer, "invalid parameter count");
			return;
		}

		shared_ptr<ITioContainer> container;

		try
		{
			unsigned int handle;

			handle = lexical_cast<unsigned int>(cmd.GetParameters()[0]);
			container = session->GetRegisteredContainer(handle);
		}
		catch(std::exception&)
		{
			MakeAnswer(error, answer, "invalid handle");
			return;
		}

		container->Clear();

		MakeAnswer(success, answer);
	}


	void TioTcpServer::OnCommand_GetRecordCount(Command& cmd, ostream& answer, size_t* moreDataSize, shared_ptr<TioTcpSession> session)
	{
		if(!CheckParameterCount(cmd, 1, exact))
		{
			MakeAnswer(error, answer, "invalid parameter count");
			return;
		}
	
		shared_ptr<ITioContainer> container;

		try
		{
			unsigned int handle;

			handle = lexical_cast<unsigned int>(cmd.GetParameters()[0]);
			container = session->GetRegisteredContainer(handle);
		}
		catch(std::exception&)
		{
			MakeAnswer(error, answer, "invalid handle");
			return;
		}

		MakeAnswer(success, answer, "count",  lexical_cast<string>(container->GetRecordCount()).c_str());
	}

	void TioTcpServer::OnCommand_SubscribeUnsubscribe(Command& cmd, ostream& answer, size_t* moreDataSize, shared_ptr<TioTcpSession> session)
	{
		if(!CheckParameterCount(cmd, 1, exact) && !CheckParameterCount(cmd, 2, exact))
		{
			MakeAnswer(error, answer, "invalid parameter count");
			return;
		}

		unsigned int handle;

		try
		{
			handle = lexical_cast<unsigned int>(cmd.GetParameters()[0]);
		}
		catch(boost::bad_lexical_cast&)
		{
			MakeAnswer(error, answer, "invalid handle");
			return;
		}

		try
		{
			string containerName, containerType;

			session->GetRegisteredContainer(handle, &containerName, &containerType);

			if(!CheckObjectAccess(containerType, containerName, cmd.GetCommand(), answer, session))
				return;

			string start;

			if(cmd.GetParameters().size() == 2)
				start = cmd.GetParameters()[1];

			if(cmd.GetCommand() == "subscribe")
			{
				session->Subscribe(handle, start);

				//
				// we'll NOT send the answer, because session::subscribe already did
				//
			}
			else if(cmd.GetCommand() == "unsubscribe")
			{
				session->Unsubscribe(handle);
				MakeAnswer(success, answer);
			}
			else
				BOOST_ASSERT(false && "only subscribe and unsubscribe, please");
		}
		catch (std::exception& e)
		{
			MakeAnswer(error, answer, e.what());
			return;
		}	
	}

	void TioTcpServer::OnCommand_Ping(Command& cmd, ostream& answer, size_t* moreDataSize, shared_ptr<TioTcpSession> session)
	{
		MakeAnswer(cmd.GetParameters().begin(), cmd.GetParameters().end(), success, answer, "pong");
	}

	void TioTcpServer::OnCommand_Version(Command& cmd, ostream& answer, size_t* moreDataSize, shared_ptr<TioTcpSession> session)
	{
		MakeAnswer(success, answer, "0.1");
	}

	bool TioTcpServer::CheckCommandAccess(const string& command, ostream& answer, shared_ptr<TioTcpSession> session)
	{
		if(auth_.CheckCommandAccess(command, session->GetTokens()) == Auth::allow)
			return true;
		
		MakeAnswer(error, answer, "access denied");
		return false;
	}

	bool TioTcpServer::CheckObjectAccess(const string& objectType, const string& objectName, const string& command, ostream& answer, shared_ptr<TioTcpSession> session)
	{
		if(auth_.CheckObjectAccess(objectType, objectName, command, session->GetTokens()) == Auth::allow)
			return true;

		MakeAnswer(error, answer, "access denied");
		return false;
	}

	string TioTcpServer::GetConfigValue(const string& key)
	{
		if(boost::to_lower_copy(key) == "userdbtype")
			return "logdb/map";

		return string();
	}

	void TioTcpServer::OnCommand_SetPermission(Command& cmd, ostream& answer, size_t* moreDataSize, shared_ptr<TioTcpSession> session)
	{
		//
		// examples:
		// set_permission persistent_map my_map push_back allow user_name
		// set_permission volatile_vector object_name * allow user_name
		//
		if(!CheckParameterCount(cmd, 4, exact) && !CheckParameterCount(cmd, 5, exact))
		{
			MakeAnswer(error, answer, "invalid parameter count");
			return;
		}

		string objectType = cmd.GetParameters()[0];
		const string& objectName = cmd.GetParameters()[1];
		const string& command = cmd.GetParameters()[2]; // command_name or *
		const string& allowOrDeny = cmd.GetParameters()[3];

		objectType = containerManager_.ResolveAlias(objectType);

		Auth::RuleResult ruleResult;

		if(allowOrDeny == "allow")
			ruleResult = Auth::allow;
		else if(allowOrDeny == "deny")
			ruleResult = Auth::deny;
		else
		{
			MakeAnswer(error, answer, "invalid permission type");
			return;
		}

		if(!containerManager_.Exists(objectType, objectName))
		{
			MakeAnswer(error, answer, "no such object");
			return;
		}

		//
		// the user_name can only be ommited when sending the __default__ rule
		//
		if(cmd.GetParameters().size() == 5)
		{
			const string& token = cmd.GetParameters()[4];

			auth_.AddObjectRule(objectType, objectName, command, token, ruleResult);
		}
		else
		{
			if(command != "__default__")
			{
				MakeAnswer(error, answer, "no user specified");
				return;
			}

			auth_.SetObjectDefaultRule(objectType, objectName, ruleResult);
		}

		MakeAnswer(success, answer);
	}

	void TioTcpServer::OnCommand_Auth(Command& cmd, ostream& answer, size_t* moreDataSize, shared_ptr<TioTcpSession> session)
	{
		if(!CheckParameterCount(cmd, 3, exact))
		{
			MakeAnswer(error, answer, "invalid parameter count");
			return;
		}

		const string& token = cmd.GetParameters()[0];
		const string& passwordType = cmd.GetParameters()[1];
		const string& password = cmd.GetParameters()[2];

		//
		// only clean password now
		//
		if(passwordType != "clean")
		{
			MakeAnswer(error, answer, "unsupported password type");
			return;
		}

		try
		{
			TioData value;
			
			metaContainers_.users->GetRecord(token, NULL, &value, NULL);

			if(TioData(password) == value)
			{
				session->AddToken(token);
				MakeAnswer(success, answer);
			}
			else
				MakeAnswer(error, answer, "invalid password");
		}
		catch (std::exception&)
		{
			MakeAnswer(error, answer, "error validating password");
		}
	}

	void TioTcpServer::OnCommand_DeleteContainer(Command& cmd, ostream& answer, size_t* moreDataSize, shared_ptr<TioTcpSession> session)
	{
		if(!CheckParameterCount(cmd, 2, exact))
		{
			MakeAnswer(error, answer, "invalid parameter count");
			return;
		}

		const string& containerType = cmd.GetParameters()[0];
		const string& containerName = cmd.GetParameters()[1];

		if(!CheckObjectAccess(containerType, containerName, cmd.GetCommand(), answer, session))
			return;

		try
		{
			containerManager_.DeleteContainer(containerType, containerName);

			MakeAnswer(success, answer);
		}
		catch (std::exception& ex)
		{
			MakeAnswer(error, answer, ex.what());
		}

		
	}

	void TioTcpServer::OnCommand_CreateContainer_OpenContainer(Command& cmd, ostream& answer, size_t* moreDataSize, shared_ptr<TioTcpSession> session)
	{
		//
		// create name type [params]
		// open name [type] [params]
		//
		if(!CheckParameterCount(cmd, 1, at_least))
		{
			MakeAnswer(error, answer, "invalid parameter count");
			return;
		}

		const string& containerName = cmd.GetParameters()[0];
		string containerType;

		if(cmd.GetParameters().size() > 1)
			containerType = cmd.GetParameters()[1];

		try
		{
			shared_ptr<ITioContainer> container;

			if(cmd.GetCommand() == "create")
			{
				if(containerName.size() > 1 && containerName[0] == '_' && containerName[1] == '_')
				{
					MakeAnswer(error, answer, "invalid name, names starting with __ are reserved for internal use");
					return;
				}

				if(!CheckCommandAccess(cmd.GetCommand(), answer, session))
					return;

				container = containerManager_.CreateContainer(containerType, containerName);
			}
			else
			{
				if(!CheckObjectAccess(containerType, containerName, cmd.GetCommand(), answer, session))
					return;

				container = containerManager_.OpenContainer(containerType, containerName);
			}

			unsigned int handle = session->RegisterContainer(containerName, container);

			MakeAnswer(success, answer, "handle", lexical_cast<string>(handle), container->GetType());
		}
		catch (std::exception& ex)
		{
			MakeAnswer(error, answer, ex.what());
		}
	}

	void TioTcpServer::OnCommand_CloseContainer(Command& cmd, ostream& answer, size_t* moreDataSize, shared_ptr<TioTcpSession> session)
	{
		using boost::bad_lexical_cast;

		if(!CheckParameterCount(cmd, 1, exact))
		{
			MakeAnswer(error, answer, "invalid parameter count");
			return;
		}

		try
		{
			session->CloseContainerHandle(lexical_cast<unsigned int>(cmd.GetParameters()[0]));
		}
		catch (std::exception&)
		{
			MakeAnswer(error, answer, "Invalid handle");
			return;
		}

		MakeAnswer(success, answer);
	}


	pair<shared_ptr<ITioContainer>, int> TioTcpServer::GetRecordBySpec(const string& spec, shared_ptr<TioTcpSession> session)
	{
		vector<string> spl;
		pair<shared_ptr<ITioContainer>, int> p;

		//
		// format: handle:rec_number
		//

		split(spl, spec, is_any_of(":"));

		if(spl.size() != 2)
			throw std::invalid_argument("invalid record spec");

		try
		{
			unsigned int handle;
			
			handle = lexical_cast<unsigned int>(spl[0]);
			
			p.second = lexical_cast<int>(spl[1]);
			
			p.first = session->GetRegisteredContainer(handle);
		}
		catch (bad_lexical_cast&)
		{
			throw std::invalid_argument("invalid record spec");
		}
		catch (std::invalid_argument) 
		{
			throw;
		}

		return p;
	}

	void TioTcpServer::OnCommand_ListHandles(Command& cmd, ostream& answer, size_t* moreDataSize, shared_ptr<TioTcpSession> session)
	{
		if(!CheckParameterCount(cmd, 0, exact))
		{
			MakeAnswer(error, answer, "invalid parameter count");
			return;
		}

		MakeAnswer(error, answer, "not implemented");

		/*
		vector<string> v;

		v.reserve(handles_.size() * 2);

		BOOST_FOREACH(HandleMap::value_type i, handles_)
		{
			unsigned int handle = i.first;
			shared_ptr<ITioContainer> container = i.second;

			v.push_back("\r\n");
			v.push_back(lexical_cast<string>(handle));
			v.push_back(container->GetName());
		}

		MakeAnswer(success, answer, "", v.begin(), v.end());
		return;
		*/
	}

	inline size_t ExtractFieldSet(
		vector<string>::const_iterator begin, 
		vector<string>::const_iterator end,
		const void* buffer,
		size_t bufferSize,
		TioData* key, 
		TioData* value, 
		TioData* metadata)
	{
		size_t fieldsTotalSize;
		vector<FieldInfo> fields;

		pair_assign(fields, fieldsTotalSize) = ExtractFieldSet(begin, end);

		if(fieldsTotalSize > bufferSize)
			return fieldsTotalSize;

		ExtractFieldsFromBuffer(fields, buffer, bufferSize, key, value, metadata);

		return 0;
	}

	
	size_t TioTcpServer::ParseDataCommand(
		Command& cmd, 
		string* containerType,
		string* containerName,
		shared_ptr<ITioContainer>* container,
		TioData* key, 
		TioData* value, 
		TioData* metadata,
		shared_ptr<TioTcpSession> session, 
		unsigned int* handle)
	{
		const Command::Parameters& parameters = cmd.GetParameters();

		if(!CheckParameterCount(cmd, 4, at_least))
			throw std::invalid_argument("Invalid parameter count");

		try
		{
			unsigned int h;

			h = lexical_cast<unsigned int>(cmd.GetParameters()[0]);
			*container = session->GetRegisteredContainer(h, containerName, containerType);

			if(handle)
				*handle = h;
		}
		catch(boost::bad_lexical_cast&)
		{
			throw std::invalid_argument("invalid handle");
		}
		catch (std::invalid_argument&)
		{
			throw std::invalid_argument("invalid handle");
		}

		return ExtractFieldSet(
			parameters.begin() + 1,
			parameters.end(),
			cmd.GetDataBuffer()->GetRawBuffer(),
			cmd.GetDataBuffer()->GetSize(),
			key,
			value,
			metadata);
	}

	void TioTcpServer::OnCommand_Pop(Command& cmd, ostream& answer, size_t* moreDataSize, shared_ptr<TioTcpSession> session)
	{
		try
		{
			if(!CheckParameterCount(cmd, 1, exact))
			{
				MakeAnswer(error, answer, "invalid parameter count");
				return;
			}

			BOOST_ASSERT(cmd.GetCommand() == "pop_back" || cmd.GetCommand() == "pop_front");

			string containerName, containerType;

			shared_ptr<ITioContainer> container = 
				session->GetRegisteredContainer(
					lexical_cast<unsigned int>(cmd.GetParameters()[0]), 
					&containerName, 
					&containerType);
			
			if(!CheckObjectAccess(containerType, containerName, cmd.GetCommand(), answer, session))
				return;

			TioData key, value, metadata;
			
			if(cmd.GetCommand() == "pop_back")
				container->PopBack(&key, &value, &metadata);
			else if(cmd.GetCommand() == "pop_front")
				container->PopFront(&key, &value, &metadata);
			
			MakeDataAnswer(key, value, metadata, answer);

		}
		catch (std::exception& e)
		{
			MakeAnswer(error, answer, e.what());
			return;
		}
	}

	void TioTcpServer::OnModify(Command& cmd, ostream& answer, size_t* moreDataSize, shared_ptr<TioTcpSession> session)
	{
		try
		{
			string containerName, containerType;
			shared_ptr<ITioContainer> container;
			TioData key, value, metadata;

			size_t dataSize = ParseDataCommand(
				cmd,
				&containerType,
				&containerName,
				&container,
				&key,
				&value,
				&metadata, 
				session);

			if(!CheckObjectAccess(containerType, containerName, cmd.GetCommand(), answer, session))
				return;

			if(dataSize != 0)
			{
				*moreDataSize = dataSize;
				return;
			}


			if(!key || !value)
			{
				MakeAnswer(error, answer, "need key and data");
				return;
			}

			if(value.GetDataType() != TioData::Sz && value.GetRawSize() < 2)
			{
				MakeAnswer(error, answer, "invalid data");
				return;
			}

			const char* sz = value.AsSz();

			//
			// only sum yet
			//
			if(sz[0] != '+')
			{
				MakeAnswer(error, answer, "invalid operation");
				return;
			}
	
			int toAdd = lexical_cast<int>(&sz[1]);

			TioData currentData;

			container->GetRecord(key, NULL, &currentData, NULL); 

			if(currentData.GetDataType() != TioData::Int)
			{
				MakeAnswer(error, answer, "modified data must be integer");
				return;
			}

			currentData.Set(currentData.AsInt() + toAdd);

			container->Set(key, currentData, TIONULL);

			MakeDataAnswer(TIONULL, currentData, TIONULL, answer);

		}
		catch (std::exception& e)
		{
			MakeAnswer(error, answer, e.what());
			return;
		}

		MakeAnswer(success, answer);

		return;
	}	

	void TioTcpServer::HandleWaitAndPop(shared_ptr<ITioContainer> container, 
		const TioData& key, const TioData& value, const TioData& metadata)
	{
		//
		// we support wait and pop just for string keys
		//
		if(key.GetDataType() != TioData::Sz)
			return;

		//
		// support for "wait and pop key" (wnp_key)
		//
		KeyPoppersPerContainerMap::iterator i = keyPoppersPerContainer_.find(GetFullQualifiedName(container));

		if(i == keyPoppersPerContainer_.end())
			return;

		//
		// anyone one waiting for this key?
		//
		KeyPoppersByKey& thisKeyPoppers = i->second;
		KeyPoppersByKey::iterator iByKey = thisKeyPoppers.find(key.AsSz());

		//
		// anyone waiting to pop?
		//
		if(iByKey != thisKeyPoppers.end() && !iByKey->second.empty())
		{
			deque<KeyPopperInfo>& q = iByKey->second;

			//
			// find first popper still alive
			//
			bool pop = false;

			while(!q.empty())
			{
				KeyPopperInfo popper = q.front(); // not a reference because we'll pop right now
				q.pop_front();

				if(popper.session.expired())
					continue;

				stringstream eventStream;

				MakeEventAnswer("wnp_key", popper.handle, key, value, metadata, eventStream);

				popper.session.lock()->SendAnswer(eventStream);

				pop = true;
				break;
			}

			if(q.empty())
				thisKeyPoppers.erase(iByKey);

			if(pop)
				container->Delete(key);
		}
	}
		

	void TioTcpServer::OnAnyDataCommand(Command& cmd, ostream& answer, size_t* moreDataSize, shared_ptr<TioTcpSession> session)
	{
		try
		{
			string containerName, containerType;
			shared_ptr<ITioContainer> container;
			TioData key, value, metadata;

			size_t dataSize = ParseDataCommand(
				cmd,
				&containerType,
				&containerName,
				&container,
				&key,
				&value,
				&metadata, 
				session);

			if(!CheckObjectAccess(containerType, containerName, cmd.GetCommand(), answer, session))
				return;

			if(dataSize != 0)
			{
				*moreDataSize = dataSize;
				return;
			}
		
			
			if(cmd.GetCommand() == "insert")
			{
				container->Insert(key, value, metadata);
				MakeAnswer(success, answer);

				HandleWaitAndPop(container, key, value, metadata);

				//
				// we already sent the answer, so, time to return
				//
				return;
			}

			else if(cmd.GetCommand() == "set")
			{
				container->Set(key, value, metadata);

				MakeAnswer(success, answer);
				
				HandleWaitAndPop(container, key, value, metadata);

				//
				// we already sent the answer, so, time to return
				//
				return;
			}

			else if(cmd.GetCommand() == "pop_back")
			{
				container->PopBack(&key, &value, &metadata);

				MakeDataAnswer(key, value, metadata, answer);

				return;
			}

			else if(cmd.GetCommand() == "pop_front")
			{
				container->PopFront(&key, &value, &metadata);

				MakeDataAnswer(key, value, metadata, answer);

				return;
			}

			else if(cmd.GetCommand() == "push_back")
			{
				container->PushBack(key, value, metadata);

				MakeAnswer(success, answer);

				//
				// support for "wait and pop next" (wnp_next)
				//
				NextPoppersMap::iterator i = nextPoppers_.find(GetFullQualifiedName(container));

				//
				// anyone waiting to pop?
				//
				if(i != nextPoppers_.end() && !i->second.empty())
				{
					deque<NextPopperInfo>& q = i->second;

					//
					// find first popper still alive
					//
					bool pop = false;

					while(!q.empty())
					{
						NextPopperInfo popper = q.front(); // not a reference because we'll pop right now
						q.pop_front();

						if(popper.session.expired())
							continue;

						stringstream eventStream;
						
						MakeEventAnswer("wnp_next", popper.handle, key, value, metadata, eventStream);

						popper.session.lock()->SendAnswer(eventStream);

						pop = true;
						break;
					}

					if(q.empty())
						nextPoppers_.erase(i);

					if(pop)
						container->PopFront(&key, &value, &metadata);
				}

				//
				// we already sent the answer, so, time to return
				//
				return;
			}

			else if(cmd.GetCommand() == "push_front")
				container->PushFront(key, value, metadata);

			else if(cmd.GetCommand() == "delete")
				container->Delete(key, value, metadata);

			else if(cmd.GetCommand() == "get")
			{
				TioData realKey;
				container->GetRecord(key, &realKey, &value, &metadata);
				
				MakeDataAnswer(realKey.GetDataType() == TioData::None ? key : realKey, value, metadata, answer);

				return;
			}

			else if(cmd.GetCommand() == "set_property")
			{
				try
				{
					container->SetProperty(key.AsSz(), value.AsSz());
				}
				catch (std::exception&)
				{
					MakeAnswer(error, answer, "key and value must be strings");
					return;
				}	
			}

			else if(cmd.GetCommand() == "get_property")
			{
				try
				{
					TioData value;

					value.Set(container->GetProperty(key.AsSz()).c_str(), true);
					
					MakeDataAnswer(key, value, TIONULL, answer);
				}
				catch (std::exception&)
				{
					MakeAnswer(error, answer, "invalid key");
					return;
				}

				return;
			}
		}
		catch (std::exception& e)
		{
			MakeAnswer(error, answer, e.what());
			return;
		}

		MakeAnswer(success, answer);
	}
	
	void TioTcpServer::Start()
	{
		DoAccept();
	}

	string TioTcpServer::GetFullQualifiedName(shared_ptr<ITioContainer> container)
	{
		return containerManager_.ResolveAlias(container->GetType())
			+ "/" + container->GetName();
	}


} // namespace tio


