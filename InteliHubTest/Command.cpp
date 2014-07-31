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

#include "pch.h"
#include "Command.h"

namespace tio
{
	
	using std::shared_ptr;
	using std::endl;

	template<typename T>
	void dump_container(const T& c, ostream& stream)
	{
		stream << "(";

		if(c.size() != 0)
		{
			typename T::const_iterator i = c.begin();

			for(;;)
			{
				stream << *i;
				++i;

				if(i == c.end())
					break;

				stream << ",";
			}
		}

		stream << ")" << endl;
		return;
	}


	Command::Command():
	separators_(" ")
	{}

	void Command::Parse(const char* source)
	{
		boost::split(params_, source, boost::is_any_of(separators_));

		if(params_.size() == 0)
		{
			params_.clear();
			command_.clear();
			throw std::invalid_argument("not a command");
		}

		source_ = source;

		command_ = params_[0];
		params_.erase(params_.begin());
	}

	const string& Command::GetCommand() const
	{
		return command_;
	}

	const string& Command::GetSource() const
	{
		return source_;
	}

	const vector<string>& Command::GetParameters() const
	{
		return params_;
	}

	void Command::Dump(ostream& stream) const
	{
		stream << command_ << endl;

		dump_container(params_, stream);
	}

	shared_ptr<tio::Buffer>& Command::GetDataBuffer()
	{
		if(!data_)
			data_ = shared_ptr<tio::Buffer>(new tio::Buffer());

		return data_;

	}
	void Command::SetDataBuffer(const shared_ptr<tio::Buffer>& data)
	{
		data_ = data;
	}
}
