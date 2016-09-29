#pragma once
#include "../InteliHubClient/tioclient_internals.h"
#include "../InteliHubClient/tioclient.hpp"

#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/thread/recursive_mutex.hpp>
#include <boost/thread/thread.hpp>
#include <boost/scoped_ptr.hpp>


#include <queue>


namespace tio
{
	namespace asio = boost::asio;
	using boost::asio::ip::tcp;
	using boost::asio::const_buffer;
	using std::vector;
	using std::queue;
	using boost::system::error_code;
	using std::string;
	using std::stringstream;
	using std::endl;
	using std::make_shared;
	
	inline string Pr1MessageDump(const char* prefix, struct PR1_MESSAGE* pr1_message)
	{
		unsigned int a;
		stringstream ret;
		char buffer[256];
		struct PR1_MESSAGE_HEADER* header = (struct PR1_MESSAGE_HEADER*)pr1_message->stream_buffer->buffer;
		struct PR1_MESSAGE_FIELD_HEADER* field_header;

		pr1_message_fill_header_info(pr1_message);
		pr1_message_parse(pr1_message);

		ret << prefix << "pr1_message: message_size=" << header->message_size 
			<< " field_count=" << header->field_count 
			<< " reserved=" << header->reserved << endl;

		for(a = 0 ; a < pr1_message->field_count ; a++)
		{
			field_header = pr1_message->field_array[a];

			ret << "  field_id=" << field_header->field_id 
				<< "(" << message_field_id_to_string(field_header->field_id) << ")"
				<< " data_type=" << field_header->data_type
				<< " data_size=" <<  field_header->data_size;
				
			ret << " value=";

			switch(field_header->data_type)
			{
			case MESSAGE_FIELD_TYPE_NONE:
				ret << "(NONE)";
				break;
			case MESSAGE_FIELD_TYPE_STRING:
				pr1_message_field_get_string(field_header, buffer, sizeof(buffer));
				ret << buffer;
				break;
			case MESSAGE_FIELD_TYPE_INT:
				ret << pr1_message_field_get_int(field_header);
				break;
			default:
				ret << "(UNKNOWN TYPE)";
				break;
			}

			if(field_header->field_id == MESSAGE_FIELD_ID_COMMAND)
				ret << ", command=" << tio_command_to_string(pr1_message_field_get_int(field_header));

			ret << endl;
		}

		return ret.str();
	}
	inline void Pr1MessageAddField(PR1_MESSAGE* message, unsigned short fieldId, int value)
	{
		pr1_message_add_field_int(message, fieldId, value);
	}

	inline void Pr1MessageAddField(PR1_MESSAGE* message, unsigned short fieldId, const string& value)
	{
		pr1_message_add_field_string(message, fieldId, value.c_str());
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


	using boost::function;
	using boost::shared_ptr;

	struct async_error_info
	{
		int error_code;
		string error_message;
		string request_command_info;

		async_error_info() : error_code(0) {}
		async_error_info(int error_code, string error_message) 
			: error_code(error_code)
			, error_message(error_message)
		{}

		async_error_info(int error_code, string error_message, string request_command_info) 
			: error_code(error_code)
			, error_message(error_message)
			, request_command_info(request_command_info)
		{}

		operator bool() const { return error_code != 0;}

	};

	typedef function<void (const async_error_info&, void*)> t_handle_callback;
	typedef function<void (const async_error_info&)> t_just_error_report_callback;
	typedef function<void (const async_error_info&, const TIO_DATA&, const TIO_DATA&, const TIO_DATA&)> t_data_callback;
	typedef function<void (const async_error_info&, int/*event_code*/, const TIO_DATA&, const TIO_DATA&, const TIO_DATA&)> t_event_callback;

	struct IAsyncContainerManager
	{
		virtual void create(const char* name, const char* type, t_handle_callback callback)=0;
		virtual void open(const char* name, const char* type, t_handle_callback callback)=0;
		virtual void close(void* handle, t_just_error_report_callback callback)=0;

		virtual void container_propset(void* handle, const struct TIO_DATA* key, const struct TIO_DATA* value, t_just_error_report_callback callback)=0;
		virtual void container_push_back(void* handle, const struct TIO_DATA* key, const struct TIO_DATA* value, const struct TIO_DATA* metadata, t_just_error_report_callback callback)=0;
		virtual void container_push_front(void* handle, const struct TIO_DATA* key, const struct TIO_DATA* value, const struct TIO_DATA* metadata, t_just_error_report_callback callback)=0;
		virtual void container_pop_back(void* handle,  t_data_callback callback)=0;
		virtual void container_pop_front(void* handle, t_data_callback callback)=0;
		virtual void container_set(void* handle, const struct TIO_DATA* key, const struct TIO_DATA* value, const struct TIO_DATA* metadata, t_just_error_report_callback callback)=0;
		virtual void container_insert(void* handle, const struct TIO_DATA* key, const struct TIO_DATA* value, const struct TIO_DATA* metadata, t_just_error_report_callback callback)=0;
		virtual void container_clear(void* handle, t_just_error_report_callback callback)=0;
		virtual void container_delete(void* handle, const struct TIO_DATA* key, t_just_error_report_callback callback)=0;
		virtual void container_get(void* handle, const struct TIO_DATA* search_key, t_data_callback callback)=0;
		virtual void container_propget(void* handle, const struct TIO_DATA* search_key, t_data_callback callback)=0;
		virtual void container_get_count(void* handle, t_data_callback callback)=0;
		virtual void container_query(void* handle, int* start, int* end, t_just_error_report_callback callback, t_event_callback query_callback)=0;
		virtual void container_subscribe(void* handle, const TIO_DATA* start, t_just_error_report_callback callback, t_event_callback event_callback)=0;
		virtual void container_unsubscribe(void* handle, t_just_error_report_callback callback)=0;
		virtual void container_wait_and_pop_next(void* handle, t_just_error_report_callback callback, t_event_callback query_callback)=0;
		virtual bool connected()=0;

		virtual IAsyncContainerManager* container_manager()=0;
	};

	class AsyncConnection : public IAsyncContainerManager
	{
		struct Pr1RequestInfo
		{
			Pr1RequestInfo()
			{
			}

			Pr1RequestInfo(shared_ptr<PR1_MESSAGE> pending_message, t_handle_callback callback)
				: pending_message(pending_message)
				, handle_callback(callback)
			{}

			Pr1RequestInfo(shared_ptr<PR1_MESSAGE> pending_message, t_data_callback callback)
				: pending_message(pending_message)
				, data_callback(callback)
			{}

			Pr1RequestInfo(shared_ptr<PR1_MESSAGE> pending_message, t_just_error_report_callback callback)
				: pending_message(pending_message)
				, just_error_report_callback(callback)
			{}

			shared_ptr<PR1_MESSAGE> pending_message;
			t_handle_callback handle_callback;
			t_just_error_report_callback just_error_report_callback;
			t_data_callback data_callback;
			string debugCallInfo;
		};

		//
		// DON'T CHANGE DECLARATION ORDER
		// because they depend on each other and are initialized on the constructor
		//
		asio::io_service* io_service_;
		tcp::socket socket_;

		std::vector< shared_ptr<PR1_MESSAGE> > pendingBinarySendData_;
		std::vector< asio::const_buffer > beingSendData_;

		std::queue< Pr1RequestInfo > waitingForAnswer_;

		std::map<void*, t_event_callback> subscriptionCallbacks_;

		t_event_callback queryCallback_;
		t_event_callback waitAndPopCallback_;

		boost::recursive_mutex mutex_;

		unsigned int magic_;

		bool useSeparatedThread_;

		boost::scoped_ptr<boost::thread> ownThread_;

		#ifdef _DEBUG
		boost::thread::id lastDataThread_;
		#endif
		

	public:

		enum UseOwnThreadMode
		{
			UseOwnThread
		};

		AsyncConnection(asio::io_service& io_service)
			: io_service_(&io_service)
			, socket_(*io_service_)
			, magic_(0xABCDABCD)
			, useSeparatedThread_(false)
		{
		}

		//
		// On this mode, we will create a separated thread to do i/o instead
		// of requiring the caller to administer io_service. It's useful to programs
		// that can't lock in io_service::run, like Win32 ones.
		// *** BEWARE THAT CALLBACKS WILL COME ON ANOTHER THREAD ***
		//
		AsyncConnection(UseOwnThreadMode)
			: io_service_(new asio::io_service())
			, socket_(*io_service_)
			, magic_(0xABCDABCD)
			, useSeparatedThread_(true)
		{

		}

		~AsyncConnection()
		{
			magic_ = 0x00000000;
		}

		bool usingSeparatedThread()
		{
			return ownThread_;
		}

		void Connect(const string& host, short port)
		{ 
			tcp::resolver resolver(*io_service_);
			tcp::resolver::query query(host, "2605");
			tcp::resolver::iterator endpoint_iterator = resolver.resolve(query);
			tcp::resolver::iterator end;

			boost::system::error_code error = boost::asio::error::host_not_found;
			while (error && endpoint_iterator != end)
			{
				socket_.close();
				socket_.connect(*endpoint_iterator++, error);
			}

			if (error)
				throw boost::system::system_error(error);

			char buffer[sizeof("going binary") -1];

			socket_.send(asio::buffer("protocol binary\r\n", sizeof("protocol binary\r\n")));
			socket_.receive(asio::buffer(buffer, sizeof(buffer)));

			// invalid answer
			if(memcmp(buffer, "going binary", sizeof("going binary")-1) !=0)
				throw std::runtime_error("Invalid answer from server during protocol negotiation");

			if(useSeparatedThread_)
			{
				ownThread_.reset(new boost::thread(boost::bind(&asio::io_service::run, io_service_)));
				io_service_->post(boost::bind(&AsyncConnection::ReadBinaryProtocolMessage, this));
			}
			else
			{
				ReadBinaryProtocolMessage();
			}
			
		}

		void Disconnect()
		{
			//
			// We will close after sendind and receiving pending commands. Note it will
			// do work the same way whatever we're running our own thread or not
			//
			io_service_->post(boost::bind(&tcp::socket::close, &socket_));

			if(ownThread_)
			{
				ownThread_->join();
				ownThread_.reset(nullptr);
			}
		}

		void OnReceivedBinaryMessage(PR1_MESSAGE* message)
		{
			CHECK_DATA_THREAD();

			bool b;
			int command;

			pr1_message_parse(message);

			b = Pr1MessageGetField(message, MESSAGE_FIELD_ID_COMMAND, &command);

			if(!b)
			{
				return;
			}

			Pr1RequestInfo requestInfo;

			switch(command)
			{
			case TIO_COMMAND_ANSWER:
				requestInfo = waitingForAnswer_.front();
				waitingForAnswer_.pop();

				if(requestInfo.just_error_report_callback)
				{
					int error_code = pr1_message_get_error_code(message);

					if(error_code != TIO_SUCCESS)
					{
						requestInfo.just_error_report_callback(async_error_info(error_code, tio_get_last_error_description(), requestInfo.debugCallInfo));
						return;
					}

					requestInfo.just_error_report_callback(async_error_info());
				}
				else if(requestInfo.handle_callback)
				{
					int error_code = pr1_message_get_error_code(message);

					if(error_code != TIO_SUCCESS)
					{
						requestInfo.handle_callback(async_error_info(error_code, tio_get_last_error_description(), requestInfo.debugCallInfo), nullptr);
						return;
					}

					PR1_MESSAGE_FIELD_HEADER* handle_field = pr1_message_field_find_by_id(message, MESSAGE_FIELD_ID_HANDLE);

					if(!handle_field || handle_field->data_type != TIO_DATA_TYPE_INT)
					{
						requestInfo.handle_callback(async_error_info(-1, "invalid answer from server"), NULL);
						return;
					}

					requestInfo.handle_callback(async_error_info(), (void*)pr1_message_field_get_int(handle_field));
				}
				else if(requestInfo.data_callback)
				{
					TIO_DATA k, v, m;
					tiodata_init(&k);
					tiodata_init(&v);
					tiodata_init(&m);

					int error_code = pr1_message_get_error_code(message);

					if(error_code != TIO_SUCCESS)
					{
						requestInfo.data_callback(async_error_info(error_code, tio_get_last_error_description(), requestInfo.debugCallInfo), k, v, m);
						return;
					}

					pr1_message_field_get_as_tio_data(message, MESSAGE_FIELD_ID_KEY, &k);
					pr1_message_field_get_as_tio_data(message, MESSAGE_FIELD_ID_VALUE, &v);
					pr1_message_field_get_as_tio_data(message, MESSAGE_FIELD_ID_METADATA, &m);
					
					requestInfo.data_callback(async_error_info(), k, v, m);
				}
				
				break;

			case TIO_COMMAND_EVENT:
				{
					PR1_MESSAGE_FIELD_HEADER* handle_field;
					PR1_MESSAGE_FIELD_HEADER* event_code_field;
					TIO_DATA key, value, metadata;

					handle_field = pr1_message_field_find_by_id(message, MESSAGE_FIELD_ID_HANDLE);
					event_code_field = pr1_message_field_find_by_id(message, MESSAGE_FIELD_ID_EVENT);

					if(handle_field &&
						handle_field->data_type == TIO_DATA_TYPE_INT &&
						event_code_field &&
						event_code_field->data_type == TIO_DATA_TYPE_INT)
					{
						void* handle = (void*)pr1_message_field_get_int(handle_field);
						int event_code = pr1_message_field_get_int(event_code_field);

						pr1_message_field_to_tio_data(pr1_message_field_find_by_id(message, MESSAGE_FIELD_ID_KEY), &key);
						pr1_message_field_to_tio_data(pr1_message_field_find_by_id(message, MESSAGE_FIELD_ID_VALUE), &value);
						pr1_message_field_to_tio_data(pr1_message_field_find_by_id(message, MESSAGE_FIELD_ID_METADATA), &metadata);

						t_event_callback callback;

						if(event_code == TIO_COMMAND_WAIT_AND_POP_NEXT)
						{
							callback = waitAndPopCallback_;
						}
						else
						{
							auto i = subscriptionCallbacks_.find(handle);

							if(i != subscriptionCallbacks_.end())
								callback = i->second;
						}

						if(callback)
							callback(async_error_info(), event_code, key, value, metadata);
				
						//
						// TODO: TIO_DATA leak
						//
					}


				}
				break;

			case TIO_COMMAND_QUERY_ITEM:
				{
					TIO_DATA key, value, metadata;

					pr1_message_field_to_tio_data(pr1_message_field_find_by_id(message, MESSAGE_FIELD_ID_KEY), &key);
					pr1_message_field_to_tio_data(pr1_message_field_find_by_id(message, MESSAGE_FIELD_ID_VALUE), &value);
					pr1_message_field_to_tio_data(pr1_message_field_find_by_id(message, MESSAGE_FIELD_ID_METADATA), &metadata);

					//
					// empty key means query is over
					//
					if(key.data_type == TIO_DATA_TYPE_NONE)
					{
						queryCallback_.clear();
						break;
					}

					if(queryCallback_)
						queryCallback_(async_error_info(), TIO_COMMAND_QUERY, key, value, metadata);
					
					//
					// TODO: TIO_DATA leak
					//
				}
				break;
			}
		}

		void OnBinaryProtocolMessage(PR1_MESSAGE* message, const error_code& err)
		{
			// this thing will delete the pointer
			shared_ptr<PR1_MESSAGE> messageHolder(message, &pr1_message_delete);

			if(CheckError(err))
				return;

			OnReceivedBinaryMessage(message);

			ReadBinaryProtocolMessage();
		}

		void OnBinaryProtocolMessageHeader(shared_ptr<PR1_MESSAGE_HEADER> header, const error_code& err)
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
				&AsyncConnection::OnBinaryProtocolMessage, 
				this, 
				message,
				asio::placeholders::error)
				);
		}

		void ReadBinaryProtocolMessage()
		{
			shared_ptr<PR1_MESSAGE_HEADER> header(new PR1_MESSAGE_HEADER);

			asio::async_read(
				socket_, 
				asio::buffer(header.get(), sizeof(PR1_MESSAGE_HEADER)),
				boost::bind(
				&AsyncConnection::OnBinaryProtocolMessageHeader, 
				this, 
				header,
				asio::placeholders::error)
				);
		}

		void SendPendingBinaryData()
		{

			CHECK_DATA_THREAD();
		
			boost::recursive_mutex::scoped_lock lock(mutex_);

			if(!beingSendData_.empty())
				return;
			
			if(pendingBinarySendData_.empty())
				return;

			for(vector< shared_ptr<PR1_MESSAGE> >::const_iterator i = pendingBinarySendData_.begin() ; 
				i != pendingBinarySendData_.end() ; 
				++i)
			{
				void* buffer;
				unsigned int bufferSize;

				pr1_message_get_buffer((*i).get(), &buffer, &bufferSize);

				beingSendData_.push_back(asio::buffer(buffer, bufferSize));
			}

			asio::async_write(
				socket_,
				beingSendData_,
				boost::bind(&AsyncConnection::OnBinaryMessageSent, this, asio::placeholders::error, asio::placeholders::bytes_transferred));
		}

		void SendBinaryMessage(Pr1RequestInfo requestInfo)
		{
			requestInfo.debugCallInfo = Pr1MessageDump("", requestInfo.pending_message.get());

			if(useSeparatedThread_)
			{
				io_service_->post([this, requestInfo]()
				{
					pendingBinarySendData_.push_back(requestInfo.pending_message);
					waitingForAnswer_.push(requestInfo);

					this->SendPendingBinaryData();
				});
			}
			else
			{
				pendingBinarySendData_.push_back(requestInfo.pending_message);
				waitingForAnswer_.push(requestInfo);
				
				SendPendingBinaryData();
			}

			
		}

		void CHECK_DATA_THREAD()
		{
#ifdef _DEBUG
			BOOST_ASSERT(lastDataThread_ == boost::thread::id() || lastDataThread_ == boost::this_thread::get_id());
			lastDataThread_ = boost::this_thread::get_id();
#endif
		}

		void OnBinaryMessageSent(const boost::system::error_code& err, size_t sent)
		{
			CHECK_DATA_THREAD();

			if(CheckError(err))
			{
				//std::cerr << "ERROR sending binary data: " << err << std::endl;
				return;
			}

			//
			// remove sent data from pending vector
			//
			pendingBinarySendData_.erase(pendingBinarySendData_.begin(), pendingBinarySendData_.begin() + beingSendData_.size());

			beingSendData_.clear();

			SendPendingBinaryData();
		}

		

		bool CheckError(const  boost::system::error_code& err)
		{
			if(!!err)
			{
				return true;
			}

			return false;
		}

		void CheckHandle(void* handle)
		{
			if(!handle)
			{
				throw std::runtime_error("Handle is null. Check if you are not calling a container method before receiving open or create callback");
			}
		}


		//////////////////////////////////////////////////////////////////////////
		//
		//
		//  IAsyncContainerManager
		//
		//
		//////////////////////////////////////////////////////////////////////////

		virtual void create(const char* name, const char* type, t_handle_callback callback)
		{
			shared_ptr<PR1_MESSAGE> msg(tio_generate_create_or_open_msg(TIO_COMMAND_CREATE, name, type), &pr1_message_delete);

			SendBinaryMessage(Pr1RequestInfo(msg, callback));

			return;
		}

		virtual void open(const char* name, const char* type, t_handle_callback callback)
		{
			shared_ptr<PR1_MESSAGE> msg(tio_generate_create_or_open_msg(TIO_COMMAND_OPEN, name, type), &pr1_message_delete);

			SendBinaryMessage(Pr1RequestInfo(msg, callback));

			return;
		}

		virtual void close(void* handle, t_just_error_report_callback callback)
		{
			CheckHandle(handle);

			shared_ptr<PR1_MESSAGE> msg(pr1_message_new(),
				&pr1_message_delete);

			pr1_message_add_field_int(msg.get(), MESSAGE_FIELD_ID_COMMAND, TIO_COMMAND_CLOSE);
			pr1_message_add_field_int(msg.get(), MESSAGE_FIELD_ID_HANDLE, (int)handle);

			SendBinaryMessage(Pr1RequestInfo(msg, callback));
			return;
		}


		virtual void container_push_back(void* handle, const struct TIO_DATA* key, const struct TIO_DATA* value, const struct TIO_DATA* metadata, t_just_error_report_callback callback)
		{
			CheckHandle(handle);

			shared_ptr<PR1_MESSAGE> msg(
				tio_generate_data_message(TIO_COMMAND_PUSH_BACK, handle, key, value, metadata),
				&pr1_message_delete);

			SendBinaryMessage(Pr1RequestInfo(msg, callback));
		}

		virtual void container_push_front(void* handle, const struct TIO_DATA* key, const struct TIO_DATA* value, const struct TIO_DATA* metadata, t_just_error_report_callback callback)
		{
			CheckHandle(handle);

			shared_ptr<PR1_MESSAGE> msg(
				tio_generate_data_message(TIO_COMMAND_PUSH_FRONT, handle, key, value, metadata),
				&pr1_message_delete);

			SendBinaryMessage(Pr1RequestInfo(msg, callback));
		}

		virtual void container_pop_back(void* handle,  t_data_callback callback)
		{
			CheckHandle(handle);

			shared_ptr<PR1_MESSAGE> msg(
				tio_generate_data_message(TIO_COMMAND_POP_BACK, handle, nullptr, nullptr, nullptr),
				&pr1_message_delete);

			SendBinaryMessage(Pr1RequestInfo(msg, callback));
		}

		virtual void container_pop_front(void* handle, t_data_callback callback)
		{
			CheckHandle(handle);

			shared_ptr<PR1_MESSAGE> msg(
				tio_generate_data_message(TIO_COMMAND_POP_FRONT, handle, nullptr, nullptr, nullptr),
				&pr1_message_delete);

			SendBinaryMessage(Pr1RequestInfo(msg, callback));
		}

		virtual void container_set(void* handle, const struct TIO_DATA* key, const struct TIO_DATA* value, const struct TIO_DATA* metadata, t_just_error_report_callback callback)
		{
			CheckHandle(handle);

			shared_ptr<PR1_MESSAGE> msg(
				tio_generate_data_message(TIO_COMMAND_SET, handle, key, value, metadata),
				&pr1_message_delete);

			SendBinaryMessage(Pr1RequestInfo(msg, callback));
		}

		virtual void container_insert(void* handle, const struct TIO_DATA* key, const struct TIO_DATA* value, const struct TIO_DATA* metadata, t_just_error_report_callback callback)
		{
			CheckHandle(handle);

			shared_ptr<PR1_MESSAGE> msg(
				tio_generate_data_message(TIO_COMMAND_INSERT, handle, key, value, metadata),
				&pr1_message_delete);

			SendBinaryMessage(Pr1RequestInfo(msg, callback));
		}

		virtual void container_clear(void* handle, t_just_error_report_callback callback)
		{
			CheckHandle(handle);

			shared_ptr<PR1_MESSAGE> msg(
				tio_generate_data_message(TIO_COMMAND_CLEAR, handle, nullptr, nullptr, nullptr),
				&pr1_message_delete);

			SendBinaryMessage(Pr1RequestInfo(msg, callback));
		}

		virtual void container_delete(void* handle, const struct TIO_DATA* key, t_just_error_report_callback callback)
		{
			CheckHandle(handle);

			shared_ptr<PR1_MESSAGE> msg(
				tio_generate_data_message(TIO_COMMAND_INSERT, handle, key, nullptr, nullptr),
				&pr1_message_delete);

			SendBinaryMessage(Pr1RequestInfo(msg, callback));
		}

		virtual void container_get(void* handle, const struct TIO_DATA* search_key, t_data_callback callback)
		{
			CheckHandle(handle);

			shared_ptr<PR1_MESSAGE> msg(
				tio_generate_data_message(TIO_COMMAND_GET, handle, search_key, nullptr, nullptr),
				&pr1_message_delete);

			SendBinaryMessage(Pr1RequestInfo(msg, callback));
		}

		virtual void container_propget(void* handle, const struct TIO_DATA* search_key, t_data_callback callback)
		{
			CheckHandle(handle);

			shared_ptr<PR1_MESSAGE> msg(
				tio_generate_data_message(TIO_COMMAND_PROPGET, handle, search_key, nullptr, nullptr),
				&pr1_message_delete);

			SendBinaryMessage(Pr1RequestInfo(msg, callback));
		}

		virtual void container_propset(void* handle, const struct TIO_DATA* key, const struct TIO_DATA* value, t_just_error_report_callback callback)
		{
			CheckHandle(handle);

			shared_ptr<PR1_MESSAGE> msg(
				tio_generate_data_message(TIO_COMMAND_PROPSET, handle, key, value, nullptr),
				&pr1_message_delete);

			SendBinaryMessage(Pr1RequestInfo(msg, callback));
		}

		virtual void container_get_count(void* handle, t_data_callback callback)
		{
			CheckHandle(handle);
			shared_ptr<PR1_MESSAGE> msg(
				tio_generate_data_message(TIO_COMMAND_COUNT, handle, nullptr, nullptr, nullptr),
				&pr1_message_delete);

			SendBinaryMessage(Pr1RequestInfo(msg, callback));
		}

		virtual void container_query(void* handle, int* start, int* end,  t_just_error_report_callback callback, t_event_callback query_callback)
		{
			CheckHandle(handle);

			shared_ptr<PR1_MESSAGE> msg(pr1_message_new(),
				&pr1_message_delete);

			pr1_message_add_field_int(msg.get(), MESSAGE_FIELD_ID_COMMAND, TIO_COMMAND_QUERY);
			pr1_message_add_field_int(msg.get(), MESSAGE_FIELD_ID_HANDLE, (int)handle);

			if(start)
				pr1_message_add_field_int(msg.get(), MESSAGE_FIELD_ID_START_RECORD, *start);

			if(end)
				pr1_message_add_field_int(msg.get(), MESSAGE_FIELD_ID_START_RECORD, *end);

			queryCallback_ = query_callback;

			SendBinaryMessage(Pr1RequestInfo(msg, callback));
		}

		virtual void container_subscribe(void* handle, const TIO_DATA* start, t_just_error_report_callback callback, t_event_callback event_callback)
		{
			CheckHandle(handle);

			shared_ptr<PR1_MESSAGE> msg(
				tio_generate_data_message(TIO_COMMAND_SUBSCRIBE, handle, start, nullptr, nullptr),
				&pr1_message_delete);

			subscriptionCallbacks_[handle] = event_callback;

			t_just_error_report_callback housekeep_on_error = [=](const async_error_info& error_info)
			{
				if(error_info)
					subscriptionCallbacks_.erase(handle);

				if(callback)
					callback(error_info);
			};

			SendBinaryMessage(Pr1RequestInfo(msg, housekeep_on_error));

			return;
		}

		virtual void container_unsubscribe(void* handle, t_just_error_report_callback callback)
		{
			CheckHandle(handle);

			shared_ptr<PR1_MESSAGE> msg(
				tio_generate_data_message(TIO_COMMAND_UNSUBSCRIBE, handle, nullptr, nullptr, nullptr),
				&pr1_message_delete);

			SendBinaryMessage(Pr1RequestInfo(msg, callback));
		}

		virtual void container_wait_and_pop_next(void* handle, t_just_error_report_callback callback, t_event_callback wnp_callback)
		{
			CheckHandle(handle);

			shared_ptr<PR1_MESSAGE> msg(pr1_message_new(),
				&pr1_message_delete);

			pr1_message_add_field_int(msg.get(), MESSAGE_FIELD_ID_COMMAND, TIO_COMMAND_WAIT_AND_POP_NEXT);
			pr1_message_add_field_int(msg.get(), MESSAGE_FIELD_ID_HANDLE, (int)handle);
			
			waitAndPopCallback_ = wnp_callback;

			SendBinaryMessage(Pr1RequestInfo(msg, callback));

			return;
		}

		virtual bool connected()
		{
			//
			// Why checking magic_ here? Because containers objects will try to close the container on destructor.
			// This code (that I'm not sure if is the best thing to do) will reduce the damage by saying that
			// the connection is not active.
			//
			//
			assert(magic_ == 0xABCDABCD);
			
			if(magic_ != 0xABCDABCD)
				return false;

			return socket_.is_open();
		}

		virtual IAsyncContainerManager* container_manager()
		{
			return this;
		}
	};




	namespace containers
	{

		template<typename TKey, typename TValue, typename TMetadata>
		void ConvertKeyValueAndMetadata(const TIO_DATA& k, const TIO_DATA& v, const TIO_DATA& m, 
			TKey* converted_k, TValue* converted_v, TMetadata* converted_m)
		{
			if(k.data_type != TIO_DATA_TYPE_NONE && converted_k)
				FromTioData(&k, converted_k);

			if(v.data_type != TIO_DATA_TYPE_NONE && converted_v)
				FromTioData(&v, converted_v);

			if(m.data_type != TIO_DATA_TYPE_NONE && converted_m)
				FromTioData(&m, converted_m);

		}

		typedef boost::function<void (const async_error_info&)> ErrorCallbackT;
		typedef boost::function<void (void)> JustDoneCallbackT;
		typedef boost::function<void (const string&, const string)> PropgetCallbackT;

		template<typename TKey, typename TValue, typename TMetadata, typename SelfT>
		class TioAsyncContainerImpl 
		{
		private: 
			// non copyable
			TioAsyncContainerImpl(const TioAsyncContainerImpl&){}
			TioAsyncContainerImpl& operator=(const TioAsyncContainerImpl&){return *this;}

		public:
			typedef TioAsyncContainerImpl<TKey, TValue, TMetadata, SelfT> this_type;
			typedef TKey key_type;
			typedef TValue value_type;
			typedef TMetadata metadata_type;
			typedef ServerValue<SelfT, TKey, TValue> server_value_type;
			typedef boost::function<void (int /*event_code*/, const TKey*, const TValue*, const TMetadata*)> EventCallbackT;
			typedef boost::function<void (const TKey*, const TValue*, const TMetadata*)> DataCallbackT;
		protected:
			void* containerHandle_;
			std::string name_;
			IAsyncContainerManager* containerManager_;
			EventCallbackT waitAndPopNextCallback_;

			bool connecting_;

			boost::mutex queuedCommandsMutex_;
			vector<function<void(void)>> queuedCommands_;

			IAsyncContainerManager* container_manager()
			{
				if(!containerManager_)
					throw std::runtime_error("container closed");

				return containerManager_;
			}

			void ExplodeIfNotConnectedOrConnecting()
			{
				if(!connected() && !connecting_)
					throw std::runtime_error("not connected");
			}

			void SendPendingCommands()
			{
				assert(!connecting_ && connected());

				vector<function<void(void)>> localQueuedCommands;

				{
					boost::mutex::scoped_lock lock(queuedCommandsMutex_);
					localQueuedCommands.swap(queuedCommands_);
				}

				for(auto i = localQueuedCommands.begin() ; i != localQueuedCommands.end() ; ++i)
				{
					function<void(void)>& f = *i;
					f();
				}

				localQueuedCommands.clear();
			}

			void QueueCommand(function<void(void)> what)
			{
				boost::mutex::scoped_lock lock(queuedCommandsMutex_);
				queuedCommands_.push_back(what);
			}

		public:
			TioAsyncContainerImpl() 
				: containerHandle_(NULL)
				, containerManager_(NULL)
				, waitAndPopNextCallback_(NULL)
				, connecting_(false)
			{
			}

			~TioAsyncContainerImpl()
			{
				if(!connected())
					return;
			
				//
				// We will try to close the container, but will fire and forget
				//
				container_manager()->close(
					containerHandle_, 
					[](const async_error_info& error_info){});
			}

			bool connected()
			{
				return !!containerManager_ && containerManager_->connected() && containerHandle_;
			}

			TIO_CONTAINER* handle()
			{
				return (TIO_CONTAINER*)containerHandle_;
			}

			void open(IAsyncContainerManager* asyncContainerManager, const string& name, JustDoneCallbackT callback, ErrorCallbackT error_callback)
			{
				containerManager_ = asyncContainerManager;

				name_ = name;

				connecting_ = true;

				container_manager()->open(
					name.c_str(), 
					NULL, 
					[this, callback, error_callback](const async_error_info& error_info, void* handle)
					{
						connecting_ = false;

						if(error_info)
						{
							if(error_callback) error_callback(error_info);
							return;
						}
						
						containerHandle_ = handle;

						SendPendingCommands();
						
						if(callback)
							callback();
					});
			}

			void create(IAsyncContainerManager* asyncContainerManager, const string& name, const string& type, 
				JustDoneCallbackT callback, ErrorCallbackT error_callback)
			{
				containerManager_ = asyncContainerManager;

				name_ = name;

				connecting_ = true;

				container_manager()->create(
					name.c_str(), 
					type.c_str(), 
					[this, callback, error_callback](const async_error_info& error_info, void* handle)
					{
						connecting_ = false;

						if(error_info)
						{
							if(error_callback) error_callback(error_info);
							return;
						}

						containerHandle_ = handle;

						//
						// I've spent sometime thinking if we should dispatch the pending commands
						// before or after calling the create/open callback.
						//
						// If we call the open/create callback *before* sending pending commands,
						// any command send on the callback will be send *before* the pending commands,
						// making it less intuitive
						//
						// BUT it will allow the callback to setup the container, like clearing it before use
						// or setting some initial values or properties that are necessary. I've choose to
						// do this
						//

						if(callback)
							callback();

						SendPendingCommands();
					});
			}

			const string& name()
			{
				return name_;
			}

			t_just_error_report_callback GenerateJustErrorReportCallback(JustDoneCallbackT callback, ErrorCallbackT errorCallback)
			{
				return [callback ,errorCallback](const async_error_info& error_info)
				{
					if(error_info)
					{
						if(errorCallback)
							errorCallback(error_info);

						return;
					}

					if(callback)
						callback();
				};

			}

			void clear(JustDoneCallbackT callback, ErrorCallbackT errorCallback)
			{
				ExplodeIfNotConnectedOrConnecting();

				auto doit = [=]()
				{
					container_manager()->container_clear(
						containerHandle_,
						GenerateJustErrorReportCallback(callback, errorCallback));
				};

				if(connecting_)
					QueueCommand(doit);
				else
					doit();
			}

			void close(JustDoneCallbackT callback, ErrorCallbackT errorCallback)
			{
				ExplodeIfNotConnectedOrConnecting();

				auto doit = [=]()
				{
					container_manager()->close(
						containerHandle_, 
						[=](const async_error_info& error_info)
						{
							if(error_info)
							{
								if(errorCallback)
									errorCallback(error_info);

								return;
							}

							if(!callback)
								return;
						
							// closed
							containerHandle_ = nullptr;

							callback();
						});
				};

				if(connecting_)
					QueueCommand(doit);
				else
					doit();
			}

			void propset(const string& key, const string& value, JustDoneCallbackT callback, ErrorCallbackT errorCallback)
			{
				ExplodeIfNotConnectedOrConnecting();

				auto doit = [=]()
				{
					container_manager()->container_propset(
						containerHandle_, 
						TioDataConverter<string>(key).inptr(),
						TioDataConverter<string>(value).inptr(),
						GenerateJustErrorReportCallback(callback, errorCallback));
				};

				if(connecting_)
					QueueCommand(doit);
				else
					doit();
			}

			void propget(const string& key, PropgetCallbackT callback, ErrorCallbackT errorCallback)
			{
				ExplodeIfNotConnectedOrConnecting();

				auto l = [callback, errorCallback](const async_error_info& error_info, const TIO_DATA& k, const TIO_DATA& v, const TIO_DATA& m)
				{
					if(error_info)
					{
						if(errorCallback)
							errorCallback(error_info);
						return;
					}

					if(!callback)
						return;

					string converted_k;
					string converted_v;

					if(!error_info)
					{
						ConvertKeyValueAndMetadata(k, v, m, &converted_k, &converted_v, (string*)nullptr);
					}

					callback(converted_k, converted_v);
				};

				auto doit = [this, l, key]()
				{
					container_manager()->container_propget(
						containerHandle_, 
						TioDataConverter<string>(key).inptr(), 
						l);
				};


				if(connecting_)
					QueueCommand(doit);
				else
					doit();
			}

			void pop_front(DataCallbackT callback, ErrorCallbackT error_callback)
			{
				container_manager()->container_pop_front(
					containerHandle_, 
					[this, callback, error_callback](const async_error_info& error_info, const TIO_DATA& k, const TIO_DATA& v, const TIO_DATA& m)
					{
						if(error_info)
						{
							if(error_callback)
								error_callback(error_info);
							return;
						}

						if(!callback)
							return;

						TKey converted_k;
						TValue converted_v;
						TMetadata converted_m;

						if(!error_info)
						{
							ConvertKeyValueAndMetadata(k, v, m, &converted_k, &converted_v, &converted_m);
						}

						callback(
							k.data_type != TIO_DATA_TYPE_NONE ? &converted_k : nullptr,
							v.data_type != TIO_DATA_TYPE_NONE ? &converted_v : nullptr,
							m.data_type != TIO_DATA_TYPE_NONE ? &converted_m : nullptr);
					}
				);
			}


			void query(int* start, int* end, JustDoneCallbackT callback, ErrorCallbackT errorCallback, EventCallbackT eventCallback)
			{
				container_manager()->container_query(
					containerHandle_,
					start,
					end,
					GenerateJustErrorReportCallback(callback, errorCallback),
					[eventCallback](const async_error_info& error_info, int event_code, const TIO_DATA& k, const TIO_DATA& v, const TIO_DATA& m)
					{
						TKey converted_k;
						TValue converted_v;
						TMetadata converted_m;

						if(!error_info)
						{
							ConvertKeyValueAndMetadata(k, v, m, &converted_k, &converted_v, &converted_m);
						}

						eventCallback(
							event_code,
							k.data_type != TIO_DATA_TYPE_NONE ? &converted_k : nullptr,
							v.data_type != TIO_DATA_TYPE_NONE ? &converted_v : nullptr,
							m.data_type != TIO_DATA_TYPE_NONE ? &converted_m : nullptr);

					});
			}

			void subscribe(const string& start, JustDoneCallbackT callback, ErrorCallbackT errorCallback, EventCallbackT eventCallback)
			{
				ExplodeIfNotConnectedOrConnecting();

				auto outerEventCallback = [=](const async_error_info& error_info, int event_code, const TIO_DATA& k, const TIO_DATA& v, const TIO_DATA& m)
				{
					TKey converted_k;
					TValue converted_v;
					TMetadata converted_m;

					if(!error_info)
					{
						ConvertKeyValueAndMetadata(k, v, m, &converted_k, &converted_v, &converted_m);
					}

					eventCallback(
						event_code,
						k.data_type != TIO_DATA_TYPE_NONE ? &converted_k : nullptr,
						v.data_type != TIO_DATA_TYPE_NONE ? &converted_v : nullptr,
						m.data_type != TIO_DATA_TYPE_NONE ? &converted_m : nullptr);

				};

				auto doit = [=]()
				{
					container_manager()->container_subscribe(
						containerHandle_,
						TioDataConverter<string>(start).inptr(),
						GenerateJustErrorReportCallback(callback, errorCallback),
						outerEventCallback
						);
				};

				if(connecting_)
					queuedCommands_.push_back(doit);
				else
					doit();
			}

			void unsubscribe(JustDoneCallbackT callback, ErrorCallbackT errorCallback)
			{
				container_manager()->container_unsubscribe(containerHandle_,
					GenerateJustErrorReportCallback(callback, errorCallback));
			}

			void set(const key_type& key, const value_type& value, const std::string* metadata, JustDoneCallbackT callback, ErrorCallbackT error_callback)
			{
				ExplodeIfNotConnectedOrConnecting();

				TioDataConverter<key_type> keyConverter(key);
				TioDataConverter<value_type> valueConverter(value);
				TioDataConverter<metadata_type> metadataConverter(metadata);

				auto doit = [this, callback, error_callback, keyConverter, valueConverter, metadataConverter]()
				{
					container_manager()->container_set(
						containerHandle_, 
						keyConverter.inptr(),
						valueConverter.inptr(),
						metadataConverter.inptr(),
						[=](const async_error_info& error_info)
						{
							if(error_info)
							{
								if(error_callback)
									error_callback(error_info);
								return;
							}

							if(!callback)
								return;

							callback();
						});
				};

				
				//
				// If we are waiting for the container to open, we will queue the set, so the
				// lib user doesn't need to wait the connection callback just to set a value
				//
				if(connecting_)
					QueueCommand(doit);
				else
					doit();
			}

			void set_string(const key_type& key, const string& value, const std::string* metadata, JustDoneCallbackT callback, ErrorCallbackT error_callback)
			{
				if(!connected())
					throw std::runtime_error("not connected");

				container_manager()->container_set(
					containerHandle_, 
					TioDataConverter<key_type>(key).inptr(),
					TioDataConverter<string>(value).inptr(),
					metadata ? TioDataConverter<std::string>(*metadata).inptr() : nullptr,
					[this, callback, error_callback](const async_error_info& error_info)
				{
						if(error_info)
						{
							if(error_callback)
								error_callback(error_info);
							return;
						}

						if(!callback)
							return;
						
						callback();
					});
			}

			void get(const key_type& search_key, DataCallbackT callback, ErrorCallbackT error_callback)
			{
				container_manager()->container_get(
					containerHandle_, 
					TioDataConverter<key_type>(search_key).inptr(),
					[this, callback, error_callback](const async_error_info& error_info, const TIO_DATA& k, const TIO_DATA& v, const TIO_DATA& m)
					{
						if(error_info)
						{
							if(error_callback)
								error_callback(error_info);
							return;
						}

						if(!callback)
							return;

						TKey converted_k;
						TValue converted_v;
						TMetadata converted_m;

						if(!error_info)
						{
							ConvertKeyValueAndMetadata(k, v, m, &converted_k, &converted_v, &converted_m);
						}

						callback(
							k.data_type != TIO_DATA_TYPE_NONE ? &converted_k : nullptr,
							v.data_type != TIO_DATA_TYPE_NONE ? &converted_v : nullptr,
							m.data_type != TIO_DATA_TYPE_NONE ? &converted_m : nullptr);
					}
				);
			}

			static string serialize(const value_type& value)
			{
				const TIO_DATA* serialized = TioDataConverter<value_type>(value).inptr();
				
				assert(serialized->data_type == TIO_DATA_TYPE_STRING);

				return string(serialized->string_, serialized->string_size_);
			}
		};

		template<typename TKey, typename TValue, typename TMetadata=std::string>
		class async_map : public TioAsyncContainerImpl<TKey, TValue, TMetadata, async_map<TKey, TValue, TMetadata> >
		{
		private:
			async_map(const async_map<TKey,TValue, TMetadata> &){}
			async_map<TKey,TValue, TMetadata>& operator=(const async_map<TKey,TValue, TMetadata> &){return *this;}
		public:
			typedef async_map<TKey, TValue, TMetadata> this_type;
			async_map(){}
		};

		template<typename TValue, typename TMetadata=std::string>
		class async_list : public TioAsyncContainerImpl<int, TValue, TMetadata, async_list<TValue, TMetadata> >
		{
		private:
			async_list(const async_list<TValue, TMetadata> &);
			async_list<TValue, TMetadata>& operator=(const async_list<TValue, TMetadata> &);
		public:
			typedef async_list<TValue, TMetadata> this_type;
			async_list(){}

			void push_back(const value_type& value, const std::string* metadata, JustDoneCallbackT callback, ErrorCallbackT error_callback)
			{
				if(!connected())
					throw std::runtime_error("not connected");

				container_manager()->container_push_back(
					containerHandle_, 
					nullptr,
					TioDataConverter<value_type>(value).inptr(),
					metadata ? TioDataConverter<std::string>(*metadata).inptr() : nullptr,
					[this, callback, error_callback](const async_error_info& error_info)
					{
						if(error_info)
						{
							if(error_callback)
								error_callback(error_info);
							return;
						}

						if(!callback)
							return;

						callback();
					});
			}

			void wait_and_pop_next(JustDoneCallbackT callback, ErrorCallbackT errorCallback, EventCallbackT eventCallback)
			{
				ExplodeIfNotConnectedOrConnecting();

				auto outerEventCallback = [eventCallback](const async_error_info& error_info, int event_code, const TIO_DATA& k, const TIO_DATA& v, const TIO_DATA& m)
				{
					int converted_k;
					TValue converted_v;
					TMetadata converted_m;

					if(!error_info)
					{
						ConvertKeyValueAndMetadata(k, v, m, &converted_k, &converted_v, &converted_m);
					}

					eventCallback(
						event_code,
						k.data_type != TIO_DATA_TYPE_NONE ? &converted_k : nullptr,
						v.data_type != TIO_DATA_TYPE_NONE ? &converted_v : nullptr,
						m.data_type != TIO_DATA_TYPE_NONE ? &converted_m : nullptr);
				};

				auto doit = [=]()
				{
					container_manager()->container_wait_and_pop_next(
						containerHandle_,
						GenerateJustErrorReportCallback(callback, errorCallback),
						outerEventCallback);
				};

				if(connecting_)
					queuedCommands_.push_back(doit);
				else
					doit();
			}
		};
	}
}


