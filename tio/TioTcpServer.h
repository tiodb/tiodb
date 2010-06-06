#pragma once

#include "pch.h"
#include "buffer.h"
#include "Command.h"
#include "ContainerManager.h"
#include "TioTcpProtocol.h"
#include "TioTcpSession.h"
#include "auth.h"

namespace tio
{
	using namespace std;
	using boost::shared_ptr;
	using boost::system::error_code;
	namespace asio = boost::asio;
	using namespace boost::asio::ip;

	std::string Serialize(const std::list<const TioData*>& fields);
	
	class TioTcpServer
	{
	public:
		typedef boost::function<void (Command&, ostream&, size_t*, shared_ptr<TioTcpSession>)> CommandFunction;

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

		typedef map< string, set<string> > RecordingSessions;
		RecordingSessions recordingSessions_;

		unsigned int lastID_;

		typedef map< string, deque<NextPopperInfo> > NextPoppersMap;
		NextPoppersMap nextPoppers_;

		typedef std::map<string, CommandFunction> CommandFunctionMap;
		CommandFunctionMap dispatchMap_;

		Auth auth_;
		
		tcp::acceptor acceptor_;
		asio::io_service& io_service_;
		
		typedef std::set< shared_ptr<TioTcpSession> > SessionsSet;
		SessionsSet sessions_;

		ContainerManager& containerManager_;

		MetaContainers metaContainers_;

		void DoAccept();
		void OnAccept(shared_ptr<TioTcpSession> client, const error_code& err);
		
		pair<shared_ptr<ITioContainer>, int> GetRecordBySpec(const string& spec, shared_ptr<TioTcpSession> session);

		size_t ParseDataCommand(Command& cmd, string* containerType, string* containerName, shared_ptr<ITioContainer>* container, 
			TioData* key, TioData* value, TioData* metadata, shared_ptr<TioTcpSession> session, unsigned int* handle = NULL);

		void LoadDispatchMap();

		void OnCommand_Start_Recording(Command& cmd, ostream& answer, size_t* moreDataSize, shared_ptr<TioTcpSession> session);
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
		void HandleWaitAndPop(shared_ptr<ITioContainer> container, 
			const TioData& key, const TioData& value, const TioData& metadata);

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

	public:
		TioTcpServer(ContainerManager& containerManager,asio::io_service& io_service, tcp::endpoint& endpoint);
		void OnClientFailed(shared_ptr<TioTcpSession> client, const error_code& err);
		void OnCommand(Command& cmd, ostream& answer, size_t* moreDataSize, shared_ptr<TioTcpSession> session);
		void Start();

		Auth& GetAuth();
		

	};

	void StartServer();
}


