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
	} //namespace MemoryStorage 
} //namespace tio

