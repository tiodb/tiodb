// tiorpc.cpp : Defines the exported functions for the DLL application.
//

#include "stdafx.h"
#include "tiorpc.h"
#include "tiorpc.pb.h"
#include <google/protobuf/descriptor.h>

using google::protobuf::RpcController;
using google::protobuf::MethodDescriptor;
using google::protobuf::Message;
using google::protobuf::Closure;
using google::protobuf::Service;
using google::protobuf::NewCallback;

using boost::shared_ptr;
using boost::scoped_ptr;
using boost::lexical_cast;

using google::protobuf::int32;
using std::string;
using std::pair;
using std::make_pair;

bool TioRpcController::Failed() const
{
	return false;
}

tio::string TioRpcController::ErrorText() const
{
	return string();
}

void TioRpcController::StartCancel()
{

}

void TioRpcController::SetFailed( const string& reason )
{

}

bool TioRpcController::IsCanceled() const
{
	return false;
}

void TioRpcController::NotifyOnCancel( Closure* callback )
{

}

//
//
//
// Client
//
//
//
//

string TioRpcClient::GenerateRequestId()
{
	return lexical_cast<string>(++lastId_);
}

TioRpcClient::TioRpcClient(tio::IContainerManager* containerManager, const char* requestContainerName, const char* responseContainerName ) 
	: lastId_(0)
	, containerManager_(containerManager)
{

	requestContainer_.create(containerManager_, requestContainerName, "volatile_list");
	responseContainer_.create(containerManager_, responseContainerName, "volatile_list");

	requestContainer_.clear();
	responseContainer_.clear();
}

void TioRpcClient::DispatchOne(TioRpcController* controller)
{
	string requestString = responseContainer_.pop_front();

	TioMessage* tioMessage = new TioMessage();

	tioMessage->ParseFromString(requestString);

	auto i = responseClosures_.find(tioMessage->request_id());

	if(i == responseClosures_.end())
	{
		//
		// NOW WHAT?
		//
		return;
	}

	const ResponseClosureInfo& responseClosureInfo = i->second;

	responseClosureInfo.response->ParseFromString(tioMessage->payload());

	responseClosureInfo.closure->Run();
}

void TioRpcClient::CallMethod(const MethodDescriptor* method, RpcController* controller, const Message* request, Message* response, Closure* done)
{
	TioMessage msg;

	msg.set_request_id(GenerateRequestId());
	msg.set_full_method_name(method->full_name());
	msg.set_type(TioMessage_Type_REQUEST);

	msg.set_response_container_name(responseContainer_.name());

	request->SerializeToString(msg.mutable_payload());

	string serializedMessage;
	msg.SerializeToString(&serializedMessage);

	responseClosures_[msg.request_id()] = ResponseClosureInfo(method, done, response);

	requestContainer_.push_back(serializedMessage);
}

TioRpcServer::TioRpcServer(tio::IContainerManager* containerManager, const char* requestContainerName)
	: containerManager_(containerManager)
{
	requestContainer_.create(containerManager_, requestContainerName, "volatile_list");
}

void TioRpcServer::RegisterService(Service* service)
{
	for(int a = 0 ; a < service->GetDescriptor()->method_count() ; a++)
	{
		const MethodDescriptor* desc = service->GetDescriptor()->method(a);
		methods_[desc->full_name()] = ServiceInfo(service, desc->name());
	}
}

void TioRpcServer::Done( TioMessage* tioMessage, Message* requestMessage, Message* responseMessage )
{
	scoped_ptr<Message> x(tioMessage), y(requestMessage), z(responseMessage);

	tio::containers::list<string> responseContainer;

	responseContainer.open(containerManager_, tioMessage->response_container_name());

	//
	// reuse message object, as recommended on google documentation
	//
	string requestId = tioMessage->request_id();

	tioMessage->Clear();

	tioMessage->set_type(TioMessage::RESPONSE);
	tioMessage->set_request_id(requestId);

	responseMessage->SerializeToString(tioMessage->mutable_payload());

	string serializedMessage;
	tioMessage->SerializeToString(&serializedMessage);

	responseContainer.push_back(serializedMessage);
}

void TioRpcServer::DispatchOne(TioRpcController* controller)
{
	OnNewMessage(string(), 0, requestContainer_.pop_front());
}

void TioRpcServer::Start()
{
	BegForMoar();
}

// 4chan says hi
void TioRpcServer::BegForMoar()
{
	requestContainer_.wait_and_pop_next(
		boost::bind(&TioRpcServer::OnNewMessage, this, _1, _2, _3));
}


void TioRpcServer::OnNewMessage(const string& /*eventName */, const size_t& index, const string& value)
{
	TioMessage* tioMessage = new TioMessage();

	tioMessage->ParseFromString(value);

	auto i = methods_.find(tioMessage->full_method_name());

	if(i == methods_.end())
		throw std::runtime_error("method not found");

	const ServiceInfo& serviceInfo = i->second;
	Service* svc = serviceInfo.service;

	const MethodDescriptor* methodDesc = svc->GetDescriptor()->FindMethodByName(serviceInfo.methodName);

	if(!methodDesc)
		throw std::runtime_error("method not found");

	Message* requestInnerMessage(svc->GetRequestPrototype(methodDesc).New());
	Message* responseMessage = svc->GetResponsePrototype(methodDesc).New();

	requestInnerMessage->ParseFromString(tioMessage->payload());

	svc->CallMethod(methodDesc, 
		NULL, 
		requestInnerMessage, 
		responseMessage, 
		NewClosure(boost::bind(&TioRpcServer::Done, this, tioMessage, requestInnerMessage, responseMessage)));

	BegForMoar();
}
