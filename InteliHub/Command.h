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
#include "buffer.h"


namespace tio
{

	using std::vector;
	using std::string;
	using std::ostream;
	using std::pair;
	using std::shared_ptr;

	class Command
	{
	public:
		typedef vector<string> Parameters;
	private:
		Parameters params_;
		string command_;
		string source_;
		const char* separators_;
		shared_ptr<tio::Buffer> data_;

	public:
		Command();
		void Parse(const char* source);
		const string& GetSource() const;
		const string& GetCommand() const;
		const Parameters& GetParameters() const;

		shared_ptr<tio::Buffer>& GetDataBuffer();
		void SetDataBuffer(const shared_ptr<tio::Buffer>& data);

		void Dump(ostream& stream) const;
	};

}
