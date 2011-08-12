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

tio::containers::map<string, string> containers;

void TIOTESTPLUGIN_API tio_plugin_start(void* p, KEY_AND_VALUE* parameters)
{
	tio::IContainerManager* containerManager = (tio::IContainerManager*)p;

	containers.open(containerManager, "meta/containers");

	const string x = containers["meta/containers"];

	containers.subscribe(&OnNewContainer);
}


// just to make sure the signature matches
tio_plugin_start_t xpto = tio_plugin_start;