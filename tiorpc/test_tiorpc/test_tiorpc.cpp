// test_tiorpc.cpp : Defines the entry point for the console application.
//

#include "stdafx.h"
#include "..\tiorpc.h"
#include "test_tiorpc.pb.h"

using google::protobuf::RpcController;
//using google::protobuf::MethodDescriptor;
using google::protobuf::Message;
using google::protobuf::Closure;
using google::protobuf::Service;
using google::protobuf::NewCallback;

using boost::shared_ptr;
using boost::scoped_ptr;
using boost::lexical_cast;

using google::protobuf::int32;
using std::string;
//using std::pair;
//using std::make_pair;
using std::cout;
using std::endl;


class MathEngineServer : public MathEngine
{
	virtual void MathOperation(RpcController* controller,
		const MathOperationRequest* request,
		MathOperationResponse* response,
		Closure* done)
	{
		int32 result = 0;

		if(request->numbers_size())
			result = request->numbers(0);

		for(int a = 1 ; a < request->numbers_size() ; a++)
		{
			switch(request->operation())
			{
			case MathOperationRequest::ADDITION:
				result += request->numbers(a);
				break;
			case MathOperationRequest::SUBTRACTION:
				result -= request->numbers(a);
				break;
			case MathOperationRequest::MULTIPLICATION:
				result *= request->numbers(a);
				break;
			case MathOperationRequest::DIVISION:
				result /= request->numbers(a);
				break;
			}
		}

		response->set_result(result);

		done->Run();
	}
};

int main()
{
	TioRpcController controller;
	MathEngineServer mathEngineServer;

	tio::Connection cn;
	cn.Connect("127.0.0.1", 6666);

	TioRpcClient rpcClient(cn.container_manager(), "mathengine_requests", "mathengine_responses");
	TioRpcServer rpcServer(cn.container_manager(), "mathengine_requests");

	rpcServer.RegisterService(&mathEngineServer);

	MathEngine::Stub mathEngine(&rpcClient);

	shared_ptr<MathOperationRequest> request(new MathOperationRequest());
	shared_ptr<MathOperationResponse> response(new MathOperationResponse());

	request->set_operation(MathOperationRequest_Operation_MULTIPLICATION);

	for(int a = 1 ; a <= 7 ; a++)
		request->add_numbers(a);

	mathEngine.MathOperation(&controller, request.get(), response.get(), 
		NewClosure(
			[request, response]()
			{
				cout << "Result=" << response->result() << "(" << request->DebugString() << ")" << endl;
			})
	);


	rpcServer.DispatchOne(&controller);
	rpcClient.DispatchOne(&controller);

	return 0;
}