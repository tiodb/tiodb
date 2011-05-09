// TioTestPlugin.cpp : Defines the exported functions for the DLL application.
//

#include "stdafx.h"
#include "TioTestPlugin.h"

using std::string;
using std::cout;
using std::endl;

void OnNewContainer(const string& eventName, const string& key, const string& value)
{
	cout << key << ":" << value << endl;

}

void TIOTESTPLUGIN_API tio_plugin_start(void* p)
{
	tio::IContainerManager* containerManager = (tio::IContainerManager*)p;
	
	tio::containers::map<string, string> containers;

	containers.open(containerManager, "meta/containers");

	const string x = containers["meta/containers"];

	containers.subscribe(&OnNewContainer);


}