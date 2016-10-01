#pragma once
#include "ContainerManager.h"

namespace tio
{
	void InitializePythonSupport(const char* programName, ContainerManager* containerManager);
	void LoadPythonPlugins(const std::vector<std::string>& plugins, const std::map<std::string, std::string>& parameters);
}