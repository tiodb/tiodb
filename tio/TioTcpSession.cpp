
#include "pch.h"
#include "TioTcpSession.h"
#include "TioTcpServer.h"

namespace tio
{
	using namespace std;
	using std::cout;
	using boost::shared_ptr;
	using boost::system::error_code;

	using boost::lexical_cast;
	using boost::bad_lexical_cast;

	using boost::split;
	using boost::is_any_of;

	using boost::tuple;

	namespace asio = boost::asio;
	using namespace boost::asio::ip;

	
	TioTcpSession::TioTcpSession(asio::io_service& io_service, TioTcpServer& server, unsigned int id) :
		socket_(io_service),
		server_(server),
		lastHandle_(0),
        pendingSendSize_(0),
		id_(id)
	{
		return;
	}
	
	TioTcpSession::~TioTcpSession()
	{
		BOOST_ASSERT(subscriptions_.empty());
		return;
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
		cout << "<< new connection" << endl;
		#endif
		ReadCommand();
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

#ifdef _TIO_DEBUG
		cout << "<< " << str << endl;
#endif

		server_.OnCommand(currentCommand_, answer, &moreDataSize, shared_from_this());
		
		if(moreDataSize)
		{
			BOOST_ASSERT(moreDataSize < 1024 * 1024);

			if(buf_.size() >= moreDataSize)
			{
				socket_.io_service().post(
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

	void TioTcpSession::OnEvent(unsigned int handle, const string& eventName, 
		const TioData& key, const TioData& value, const TioData& metadata)
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

		char* buffer = new char[answerSize+1];
		strcpy(buffer, str.c_str());

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
            return;

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

		handles_.erase(i);
		
		Unsubscribe(handle);
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

		shared_ptr<SUBSCRIPTION_INFO> subscriptionInfo(new SUBSCRIPTION_INFO);
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
				boost::bind(&TioTcpSession::OnEvent, shared_from_this(), handle, _1, _2, _3, _4), start);
			
			SendString("answer ok\r\n");
		}
		catch(std::exception& ex)
		{
			subscriptions_.erase(handle);
			SendString(string("answer error ") + ex.what() + "\r\n");
		}

		return;
	}

	void TioTcpSession::SendPendingSnapshots()
	{
		if(pendingSnapshots_.empty())
			return;

		//
		// TODO: hardcoded counter
		//
		for(unsigned int a = 0 ; a < 64 ; a++)
		{
			if(pendingSnapshots_.empty())
				return;

			std::list<unsigned int> toRemove;

			BOOST_FOREACH(SubscriptionMap::value_type& p, pendingSnapshots_)
			{
				unsigned int handle;
				shared_ptr<SUBSCRIPTION_INFO> info;
				TioData searchKey, key, value, metadata;

				pair_assign(handle, info) = p;

				if(info->resultSet)
				{
					bool b;
					
					b = info->resultSet->GetRecord(&key, &value, &metadata);

					if(!b || !info->resultSet->MoveNext())
					{
						// done
						info->cookie = info->container->Subscribe(
							boost::bind(&TioTcpSession::OnEvent, shared_from_this(), handle, _1, _2, _3, _4), "");

						toRemove.push_back(handle);
					}
				}
				else
				{
					unsigned int recordCount = info->container->GetRecordCount();

					if(recordCount == 0 || info->nextRecord >= recordCount)
					{
						// done
						info->cookie = info->container->Subscribe(
							boost::bind(&TioTcpSession::OnEvent, shared_from_this(), handle, _1, _2, _3, _4), "");

						toRemove.push_back(handle);
						continue;
					}

					ASSERT(info->cookie == 0);

					searchKey = static_cast<int>(info->nextRecord);

					//
					// TODO: this is slow when sending a linked list, we're accessing by index.
					// I don't see how we can make it faster without making it a special case...
					//
					info->container->GetRecord(searchKey, &key, &value, &metadata);
				}

				//
				// TODO: maybe this heuristic is not good. I'm assuming a map being accessed
				// item index will return a different key than the searched. We'll pass the
				// numeric key and it will return the real string key
				//
				//
				OnEvent(handle, searchKey == key ? "push_back" : "insert",
					key, value, metadata);

				info->nextRecord++;
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
} // namespace tio


