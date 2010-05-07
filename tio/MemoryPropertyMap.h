#pragma once
#include "Container.h"


namespace tio 
{ 
	namespace MemoryStorage
	{
		using namespace std;
		using boost::shared_ptr;
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
	} //namespace MemoryStorage 
} //namespace tio

