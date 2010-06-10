#pragma once

#include "Container.h"
#include "Command.h"
//#include "TioTcpServer.h"

namespace tio
{
	using namespace std;
	using boost::shared_ptr;
	using boost::weak_ptr;
	using boost::system::error_code;

	namespace asio = boost::asio;
	using namespace boost::asio::ip;

	class TioTcpServer;

	class TioTcpSession : 
		public boost::enable_shared_from_this<TioTcpSession>,
		public boost::noncopyable
	{
	private:
		unsigned int id_;
		tcp::socket socket_;
		TioTcpServer& server_;

		Command currentCommand_;

		asio::streambuf buf_;

		typedef std::map<unsigned int, pair<shared_ptr<ITioContainer>, string> > HandleMap;

		//               handle             container                  subscription cookie
		typedef std::map<unsigned int, pair<shared_ptr<ITioContainer>, unsigned int> > DiffMap;

		HandleMap handles_;
		
		DiffMap diffs_;

		unsigned int lastHandle_;
        unsigned int pendingSendSize_;

        std::queue<std::string> pendingSendData_;

		struct SUBSCRIPTION_INFO
		{
			SUBSCRIPTION_INFO()
			{
				cookie = 0;
				nextRecord = 0;
			}

			unsigned int cookie;
			unsigned int nextRecord;
			shared_ptr<ITioContainer> container;
			shared_ptr<ITioResultSet> resultSet;
		};

		//                  handle
		typedef std::map<unsigned int, shared_ptr<SUBSCRIPTION_INFO> > SubscriptionMap;
		SubscriptionMap subscriptions_;
		SubscriptionMap pendingSnapshots_;

		vector<string> tokens_;

		void SendString(const string& str);
		void SendStringNow(const string& str);
		
        void UnsubscribeAll();

		void SendPendingSnapshots();

		void SendResultSetItem(unsigned int queryID, 
			const TioData& key, const TioData& value, const TioData& metadata);

	public:

		TioTcpSession(asio::io_service& io_service, TioTcpServer& server, unsigned int id);
		~TioTcpSession();
		void LoadDispatchMap();

		tcp::socket& GetSocket();
		void OnAccept();
		void ReadCommand();

		unsigned int GetID();

		void SendResultSet(shared_ptr<ITioResultSet> resultSet, unsigned int queryID);

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

		void OnEvent(unsigned int handle, const string& eventName, const TioData& key, const TioData& value, const TioData& metadata);
		void Subscribe(unsigned int handle, const string& start);
		void Unsubscribe(unsigned int handle);

		const vector<string>& GetTokens();
		void AddToken(const string& token);

		shared_ptr<ITioContainer> GetDiffDestinationContainer(unsigned int handle);
		void SetupDiffContainer(unsigned int handle, shared_ptr<ITioContainer> destinationContainer);
		void StopDiffs();

		void SetCommandRunning()
		{
			commandRunning_ = true;
		}
		void UnsetCommandRunning()
		{
			commandRunning_ = false;
		}

		bool commandRunning_;
	};		
}
