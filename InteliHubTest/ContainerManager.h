/*
Tio: The Information Overlord
Copyright 2010 Rodrigo Strauss (http://www.1bit.com.br)

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
#pragma once

#include "Container.h"

namespace tio
{
	using std::shared_ptr;
	using std::weak_ptr;

	class ContainerManager
	{
		typedef std::map<string, shared_ptr<ITioStorageManager> > ManagerByType;
		typedef map< string, string > AliasesMap;
		typedef map< string, weak_ptr<ITioContainer> > OpenContainersMap;

		tio::recursive_mutex bigLock_;
		ManagerByType managerByType_;
		AliasesMap aliases_;

		OpenContainersMap openContainers_;

		enum OperationType
		{
			create,
			open
		};

        shared_ptr<ITioContainer> meta_containers_, meta_availableTypes_;

		shared_ptr<ITioContainer> CreateOrOpen(string type, OperationType op, const string& name);

		shared_ptr<ITioStorageManager> GetStorageManagerByType(string type);
	public:

		void AddAlias(const string& alias, const string& type);
		
		void RegisterFundamentalStorageManagers( shared_ptr<ITioStorageManager> volatileList, shared_ptr<ITioStorageManager> volatileMap);
		void RegisterStorageManager(const string& type, shared_ptr<ITioStorageManager> manager);
	
		shared_ptr<ITioContainer> CreateContainer(const string& type, const string& name);
		shared_ptr<ITioContainer> OpenContainer(const string& type, const string& name);

		void DeleteContainer(const string& type, const string& name);

		bool Exists(const string& containerType, const string& containerName);

		string ResolveAlias(const string& type);
	};
}
