#pragma once

#include <string>
#include <map>

#include <boost/shared_ptr.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/function.hpp>
#include <boost/lexical_cast.hpp>

#include <google/protobuf/stubs/common.h>
#include <google/protobuf/service.h>

#include <tioclient.hpp>

#include "tiorpc.pb.h"

#ifdef TIORPC_EXPORTS
#define TIORPC_API __declspec(dllexport)
#else
#define TIORPC_API __declspec(dllimport)
#endif

// This class is exported from the tiorpc.dll
class TIORPC_API Ctiorpc {
public:
	Ctiorpc(void);
	// TODO: add your methods here.
};

extern TIORPC_API int ntiorpc;

TIORPC_API int fntiorpc(void);



class OnceClosure : public google::protobuf::Closure {
public:
	OnceClosure(const boost::function0<void> &f) : f_(f) {
	}
	void Run() {
		f_();
		delete this;
	}
private:
	boost::function0<void> f_;
};

class PermenantClosure : public google::protobuf::Closure {
public:
	PermenantClosure(const boost::function0<void> &f) : f_(f) {
	}
	void Run() {
		f_();
	}
private:
	boost::function0<void> f_;
};


inline google::protobuf::Closure *NewClosure(
	const boost::function0<void> &f) {
		return new OnceClosure(f);
}

inline google::protobuf::Closure *NewPermenantClosure(
	const boost::function0<void> &f) {
		return new PermenantClosure(f);
}

class TIORPC_API TioRpcController : public google::protobuf::RpcController 
{
public:

	TioRpcController(){}
	virtual void Reset(){}

	virtual bool Failed() const;

	virtual std::string ErrorText() const;

	virtual void StartCancel();

	virtual void SetFailed(const std::string& reason);

	virtual bool IsCanceled() const;

	virtual void NotifyOnCancel(google::protobuf::Closure* callback);

private:
	GOOGLE_DISALLOW_EVIL_CONSTRUCTORS(TioRpcController);
};


class TIORPC_API TioRpcClient : public google::protobuf::RpcChannel 
{
	tio::IContainerManager* containerManager_;

	tio::containers::list<std::string> requestContainer_;
	tio::containers::list<std::string> responseContainer_;

	struct ResponseClosureInfo
	{
		ResponseClosureInfo(){}

		ResponseClosureInfo(const google::protobuf::MethodDescriptor* methodDescription, google::protobuf::Closure* closure, google::protobuf::Message* response)
			: methodDescription(methodDescription), closure(closure), response(response)
		{}

		const google::protobuf::MethodDescriptor* methodDescription;
		google::protobuf::Closure* closure;
		google::protobuf::Message* response;
	};

	std::map<std::string, ResponseClosureInfo> responseClosures_;
	unsigned int lastId_;

	std::string GenerateRequestId();

public:

	TioRpcClient(tio::IContainerManager* containerManager, const char* requestContainerName, const char* responseContainerName);

	void DispatchOne(TioRpcController* controller);

	virtual void CallMethod(const google::protobuf::MethodDescriptor* method,
		google::protobuf::RpcController* controller,
		const google::protobuf::Message* request,
		google::protobuf::Message* response,
		google::protobuf::Closure* done);
};

class TIORPC_API TioRpcServer
{
	struct ServiceInfo
	{
		google::protobuf::Service* service;
		std::string methodName;

		ServiceInfo(google::protobuf::Service* service, std::string methodName) 
			: service(service), methodName(methodName)
		{}

		ServiceInfo(){}
	};

	tio::IContainerManager* containerManager_;
	tio::containers::list<std::string> requestContainer_;

	std::map<std::string, ServiceInfo> methods_;

	void OnNewMessage(const std::string& /*eventName */, const size_t& index, const std::string& value);

public:

	TioRpcServer(tio::IContainerManager* containerManager, const char* requestContainerName);

	void RegisterService(google::protobuf::Service* service);

	void Done(TioMessage* tioMessage, google::protobuf::Message* requestMessage, google::protobuf::Message* responseMessage);

	void DispatchOne(TioRpcController* controller);

	void Start();
	void BegForMoar();
};