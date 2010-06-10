#include "pch.h"
#include "ContainerManager.h"

namespace tio
{
	void ContainerManager::RegisterFundamentalStorageManagers(
		shared_ptr<ITioStorageManager> volatileList,
		shared_ptr<ITioStorageManager> volatileMap)
	{
		managerByType_["volatile_list"] = volatileList;
		managerByType_["volatile_map"] = volatileMap;

		meta_containers_ = CreateContainer("volatile_map", "meta/containers");
		meta_availableTypes_ = CreateContainer("volatile_list", "meta/available_types");

		//
		// it'll make it available on the meta/containers list itself...
		//
		meta_containers_ = CreateContainer("volatile_map", "meta/containers");
		

		meta_availableTypes_->PushBack(TIONULL, "volatile_list");
		meta_availableTypes_->PushBack(TIONULL, "volatile_map");
	}

	void ContainerManager::RegisterStorageManager(const string& type, shared_ptr<ITioStorageManager> manager)
	{
		managerByType_[type] = manager;

		meta_availableTypes_->PushBack(TIONULL, type);

        std::vector<StorageInfo> storageList = manager->GetStorageList();

        BOOST_FOREACH(StorageInfo& si, storageList)
        {
            meta_containers_->Set(si.name, si.type);
        }
	}


	shared_ptr<ITioStorageManager> ContainerManager::GetStorageManagerByType(string type)
	{
		type = ResolveAlias(type);

		ManagerByType::iterator i = managerByType_.find(type);

		if(i == managerByType_.end())
			throw std::invalid_argument("invalid type");

		return i->second;
	}

	//
	// we'll always create a Container, even if we're been asked to open the
	// same one. It's like a file system. If you open a file several times, you open the same
	// file, but using a different handle. Some properties (like file pointer, locks, etc) 
	// are associated to the handle, not to the file. Container instance is a handle.
	//
	shared_ptr<ITioContainer> ContainerManager::CreateOrOpen(string type, OperationType op, const string& name)
	{
		type = ResolveAlias(type);

		shared_ptr<ITioStorage> storage;
		shared_ptr<ITioPropertyMap> propertyMap;

		if(op == create)
		{
			shared_ptr<ITioStorageManager> storageManager = GetStorageManagerByType(type);

			pair_assign(storage, propertyMap) = storageManager->CreateStorage(type, name);
			
			if(meta_containers_)
				meta_containers_->Set(name, type);
		}
		else if (op == open)
		{
			if(type.empty())
			{
				if(!meta_containers_)
					throw std::runtime_error("can't find the type for this container");

				TioData value;
				meta_containers_->GetRecord(name, NULL, &value);
				type = value.AsSz();
			}

			shared_ptr<ITioStorageManager> storageManager = GetStorageManagerByType(type);

			pair_assign(storage, propertyMap) = storageManager->OpenStorage(type, name);
		}

		return shared_ptr<ITioContainer>(new Container(storage, propertyMap));
	}

	void ContainerManager::DeleteContainer(const string& type, const string& name)
	{
		string realType = ResolveAlias(type);
		shared_ptr<ITioStorageManager> storageManager = GetStorageManagerByType(realType);

		storageManager->DeleteStorage(realType, name);

		meta_containers_->Delete(name);
	}


	shared_ptr<ITioContainer> ContainerManager::CreateContainer(const string& type, const string& name)
	{
		return CreateOrOpen(type, create, name);
	}

	shared_ptr<ITioContainer> ContainerManager::OpenContainer(const string& type, const string& name)
	{
		return CreateOrOpen(type, open, name);
	}

	void ContainerManager::AddAlias(const string& alias, const string& type)
	{
		aliases_[alias] = type;

	}

	bool ContainerManager::Exists(const string& containerType, const string& containerName)
	{
		return GetStorageManagerByType(containerType)->Exists(containerType, containerName);
	}

	string ContainerManager::ResolveAlias(const string& type)
	{
		AliasesMap::const_iterator iAlias = aliases_.find(type);

		if(iAlias != aliases_.end())
			return iAlias->second;
		else
			return type;
	}
}
