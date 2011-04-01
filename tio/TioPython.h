#pragma once
#include "ContainerManager.h"

namespace tio
{
	void InitializePythonSupport(const char* programName, ContainerManager* containerManager);
	void LoadPythonPlugins(const std::vector<std::string>& plugins);
}