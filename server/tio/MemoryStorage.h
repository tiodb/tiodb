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
#include "VectorStorage.h"
#include "MapStorage.h"
#include "ListStorage.h"


namespace tio 
{ 
namespace MemoryStorage
{
	
	using std::shared_ptr;
	using boost::lexical_cast;

	class MemoryPropertyMap : public ITioPropertyMap
	{
		typedef map<string, string> property_map;
		ITioPropertyMap* specialPropertiesMap_;
		property_map data_;

	public:

		MemoryPropertyMap(ITioPropertyMap* specialPropertiesMap = NULL):
		  specialPropertiesMap_(specialPropertiesMap)
		{}

		virtual string Get(const string& key)
		{
			//
			// special properties starts with __
			// e.g. __keys__
			//
			if(specialPropertiesMap_ && key.size() > 2 && key[0] == '_' && key[1] == '_')
			{
				return specialPropertiesMap_->Get(key);
			}

			property_map::const_iterator i = data_.find(key);

			if(i == data_.end())
				throw std::invalid_argument("key not found");

			return i->second;
		}

		virtual void Set(const string& key, const string& value)
		{
			data_[key] = value;
		}
	};

	pair<shared_ptr<ITioStorage>, shared_ptr<ITioPropertyMap> > CreateVectorStorage(const string& name, const string& type)
	{
		pair<shared_ptr<ITioStorage>, shared_ptr<ITioPropertyMap> > p;
		VectorStorage* storage = new VectorStorage(name, type);
		MemoryPropertyMap* propertyMap = new MemoryPropertyMap();

		p.first = shared_ptr<ITioStorage>(storage);
		p.second = shared_ptr<ITioPropertyMap>(propertyMap);

		return p;
	}

	pair<shared_ptr<ITioStorage>, shared_ptr<ITioPropertyMap> > CreateMapStorage(const string& name, const string& type)
	{
		pair<shared_ptr<ITioStorage>, shared_ptr<ITioPropertyMap> > p;
		MapStorage* storage = new MapStorage(name, type);
		MemoryPropertyMap* propertyMap = new MemoryPropertyMap(storage);

		p.first = shared_ptr<ITioStorage>(storage);
		p.second = shared_ptr<ITioPropertyMap>(propertyMap);

		return p;
	}

	pair<shared_ptr<ITioStorage>, shared_ptr<ITioPropertyMap> > CreateListStorage(const string& name, const string& type)
	{
		pair<shared_ptr<ITioStorage>, shared_ptr<ITioPropertyMap> > p;
		ListStorage* storage = new ListStorage(name, type);
		MemoryPropertyMap* propertyMap = new MemoryPropertyMap();

		p.first = shared_ptr<ITioStorage>(storage);
		p.second = shared_ptr<ITioPropertyMap>(propertyMap);

		return p;
	}
	
	class MemoryStorageManager: public ITioStorageManager
	{
		struct StorageInfoEx: public StorageInfo
		{
			shared_ptr<ITioStorage> storage;
			shared_ptr<ITioPropertyMap> propertyMap;
		};

		typedef std::map<string, StorageInfoEx> StorageMap;
		StorageMap containers_;
		
		typedef map<string, pair<shared_ptr<ITioStorage>, shared_ptr<ITioPropertyMap> > (*) (const string&, const string&)> SupportedTypesMap;
		SupportedTypesMap supportedTypes_;

		EventSink sink_;

	public:

		virtual void SetSubscriber(EventSink sink)
		{
			sink_ = sink;

			for (auto& v : containers_)
				v.second.storage->SetSubscriber(sink_);
		}

		virtual void RemoveSubscriber()
		{
			sink_ = nullptr;
			for (auto& v : containers_)
				v.second.storage->SetSubscriber(nullptr);
		}

		MemoryStorageManager()
		{
			supportedTypes_["volatile_vector"] = &CreateVectorStorage;
			supportedTypes_["volatile_map"] = &CreateMapStorage;
			supportedTypes_["volatile_list"] = &CreateListStorage;
		}

		virtual std::vector<string> GetSupportedTypes()
		{
			std::vector<string> ret;

			BOOST_FOREACH(SupportedTypesMap::value_type& p, supportedTypes_)
				ret.push_back(p.first);

			return ret;
		}

		inline bool Exists(const string& containerType, const string& containerName)
		{
			const string& fullName = GenerateName(containerType, containerName);
			return key_found(containers_, fullName);
		}

		inline string GenerateNamelessName()
		{
			static unsigned int nameless = 0;
			return string("__nameless") + lexical_cast<string>(nameless) + "__";
		}

		string GenerateName(const string& containerType, const string& containerName)
		{
			return containerType + "/" + containerName;
		}

		void DeleteStorage(const string& containerType, const string& containerName)
		{
			size_t deleteCount = containers_.erase(GenerateName(containerType, containerName));

			if(deleteCount == 0)
				throw std::invalid_argument("no such data container");
		}

		virtual pair<shared_ptr<ITioStorage>, shared_ptr<ITioPropertyMap> > 
		 	CreateStorage(const string& type, const string& name)
		{
			SupportedTypesMap::iterator i = supportedTypes_.find(type);
			
			if(i == supportedTypes_.end())
				throw std::invalid_argument("storage type not supported");

			pair<shared_ptr<ITioStorage>, shared_ptr<ITioPropertyMap> > ret;

			string actualName = name.empty() ? GenerateNamelessName() : name;

			//pair< shared_ptr<ITioStorage>, shared_ptr<ITioPropertyMap> >& p = containers_[GenerateName(type, actualName)];
			StorageInfoEx& info = containers_[GenerateName(type, actualName)];

			//
			// check if it already exists
			//
			if(!info.storage)
			{
				ret = i->second(actualName, type);
				
				info.storage = ret.first;
				info.propertyMap = ret.second;
				info.name = name;
				info.type = type;
			}
			else
			{
				ret.first = info.storage;
				ret.second = info.propertyMap;
			}

			ret.first->SetSubscriber(sink_);
			
			return ret;
		}

		virtual pair<shared_ptr<ITioStorage>, shared_ptr<ITioPropertyMap> > 
			OpenStorage(const string& type, const string& name)
		{
			if(!key_found(supportedTypes_, type))
				throw std::invalid_argument("storage type not supported");

			StorageMap::iterator i = containers_.find(type + "/" + name);

			if(i == containers_.end())
				throw std::invalid_argument("no such data storage");

			pair<shared_ptr<ITioStorage>, shared_ptr<ITioPropertyMap> > ret;

			ret.first = i->second.storage;
			ret.second = i->second.propertyMap;

			ret.first->SetSubscriber(sink_);

			return ret;
		}

		virtual vector<StorageInfo> GetStorageList()
		{
			vector<StorageInfo> ret;

			ret.reserve(containers_.size());

            for(StorageMap::const_iterator i = containers_.begin() ; i != containers_.end() ; ++i)
            {
                ret.push_back(i->second);
            }

			return ret;
		}

	};
} //namespace MemoryStorage 
} //namespace tio

