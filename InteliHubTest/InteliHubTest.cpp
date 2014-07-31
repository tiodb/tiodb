// InteliHubTest.cpp : Defines the entry point for the console application.
//

#include "pch.h"
#include "ContainerManager.h"
#include "MemoryStorage.h"

using namespace tio;
using namespace std;


int _tmain(int argc, _TCHAR* argv[])
{
	ContainerManager mgr;

	// precisa ter algum storage manager; felizmente já tem implementação
	shared_ptr<ITioStorageManager> mem =
		shared_ptr<ITioStorageManager>(new MemoryStorage::MemoryStorageManager());

	mgr.RegisterFundamentalStorageManagers(mem, mem);

	if (auto container = mgr.CreateContainer("volatile_list", "test"))
	{
		container->PushBack(TIONULL, "value1");
		container->PushBack(TIONULL, "value2");
		container->PushBack(TIONULL, "value3");
		container->PushBack(TIONULL, "value4");
	}

	return 0;
}
