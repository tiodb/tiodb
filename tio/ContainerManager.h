#pragma once

#include "Container.h"

namespace tio
{
	using boost::shared_ptr;
	using boost::weak_ptr;

	class ContainerManager
	{
		typedef std::map<string, shared_ptr<ITioStorageManager> > ManagerByType;
		typedef map< string, string > AliasesMap;

		ManagerByType managerByType_;
		AliasesMap aliases_;

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
