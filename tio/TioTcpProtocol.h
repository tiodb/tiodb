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


namespace tio
{
	using std::string;
	using std::stringstream;
	using std::ostream;
	using boost::lexical_cast;
	using boost::bad_lexical_cast;
	
	enum AnswerType
	{
		success,
		error
	};

	inline void MakeAnswerStart(AnswerType type, ostream& stream, const char* answer = NULL)
	{
		stream << "answer " << (type == success ? "ok" : "error") << " ";
		if(answer && *answer)
			stream << answer;
	}

	inline void MakeAnswerEnd(ostream& stream)
	{
		stream << "\r\n" << std::flush;
	}

	inline void MakeAnswer(AnswerType type, ostream& stream, const char* answer = NULL)
	{
		MakeAnswerStart(type, stream, answer);
		MakeAnswerEnd(stream);
	}

	inline void MakeAnswer(AnswerType type, ostream& stream, const string& answer)
	{
		MakeAnswer(type, stream, answer.c_str());
	}

	inline void MakeAnswer(AnswerType type, ostream& stream, const string& answer1, const string& answer2)
	{
		MakeAnswer(type, stream, answer1 + " " + answer2);
	}

	inline void MakeAnswer(AnswerType type, ostream& stream, const string& answer1, const string& answer2, const string& answer3)
	{
		MakeAnswerStart(type, stream);
		stream << answer1 << " " << answer2 << " " << answer3;
		MakeAnswerEnd(stream);
	}

	template<typename T>
	inline void MakeAnswer(T begin, T end, AnswerType type, ostream& stream, const char* answer)
	{
		MakeAnswerStart(type, stream, answer);

		BOOST_FOREACH(const string& str, std::make_pair(begin, end))
			stream << " " << str;

		MakeAnswerEnd(stream);
	}

	inline void SerializeData(const TioData& key, const TioData& value, const TioData& metadata, ostream& stream)
	{
		if(!key && !value && !metadata)
			return;

		stringstream buffer;
		string keyString, valueString, metadataString;

		const TioData* data[] = {&key, &value, &metadata};
		const char* names[] = {"key", "value", "metadata"};
		string* strings[] = {&keyString, &valueString, &metadataString};

		for(int a = 0 ; a < 3 ; a++)
		{
			const TioData* currentData = data[a];
			string* currentString = strings[a];
			const char* currentName = names[a];

			if(!currentData || currentData->Empty())
				continue;

			buffer.str("");

			buffer << *currentData;

			*currentString = buffer.str();

			stream << " " << currentName 
				   << " " << GetDataTypeAsString(*currentData)
				   << " " << currentString->length();
		}

		stream << "\r\n";

		for(int a = 0 ; a < 3 ; a++)
		{
			const TioData* currentData = data[a];
			const string* currentString = strings[a];

			if(!currentData || currentData->Empty())
				continue;

			stream << *currentString << "\r\n";
		}
	}

	inline void MakeEventAnswer(const string& eventName, unsigned int handle, 
		const TioData& key, const TioData& value, const TioData& metadata, ostream& stream)
	{
		stream << "event " << lexical_cast<string>(handle) << " " << eventName;

		SerializeData(key, value, metadata, stream);
	}

	inline void MakeDataAnswer(const TioData& key, const TioData& value, const TioData& metadata, ostream& stream)
	{
		MakeAnswerStart(success, stream, "data");

		SerializeData(key, value, metadata, stream);
	}

	inline bool IsValidFieldName(const string& fieldName)
	{
		return fieldName == "key" || fieldName == "value" || fieldName == "metadata";
	}

	struct FieldInfo
	{
		string name;
		string type;
		size_t size;
	};

	inline void SetTioData(TioData* tioData, const FieldInfo& fieldInfo, const unsigned char* buffer)
	{
		string str(buffer, buffer + fieldInfo.size);

		//
		// TODO: replace lexical_cast with something else, it's very slow
		//
		if(fieldInfo.type == "int")
			tioData->Set(lexical_cast<int>(str));
		else if(fieldInfo.type == "double")
			tioData->Set(lexical_cast<double>(str));
		else if(fieldInfo.type == "string")
			tioData->Set(str.c_str(), str.size());
		else
			throw std::invalid_argument("invalid data type");
	}


	inline void ExtractFieldsFromBuffer(
		const vector<FieldInfo>& fields, 
		const void* buffer,
		size_t bufferSize,
		TioData* key, 
		TioData* value, 
		TioData* metadata)
	{
		unsigned char* rawBuffer = (unsigned char*)buffer;

		for(vector<FieldInfo>::const_iterator i = fields.begin() ; i != fields.end() ; ++i)
		{
			const FieldInfo& fieldInfo = *i;
			BOOST_ASSERT(rawBuffer <= rawBuffer + bufferSize);

			TioData* currentFieldData = NULL;

			if(fieldInfo.name == "key")
				currentFieldData = key;
			else if(fieldInfo.name == "value")
				currentFieldData = value;
			else if(fieldInfo.name == "metadata")
				currentFieldData = metadata;

			if(currentFieldData)
				SetTioData(currentFieldData, fieldInfo, rawBuffer);
			//
			// + 2 for ending \r\n
			//
			rawBuffer += fieldInfo.size + 2;
		}
	}

	inline std::pair<vector<FieldInfo>, size_t> ExtractFieldSet(
		vector<string>::const_iterator begin, 
		vector<string>::const_iterator end)
	{
		vector<FieldInfo> fields;
		size_t totalSize = 0;

		while(begin != end)
		{
			FieldInfo fieldInfo;

			//
			// field name
			//
			fieldInfo.name = *begin;

			if(!IsValidFieldName(fieldInfo.name))
				throw std::invalid_argument("invalid field name");

			if(++begin == end)
				throw std::invalid_argument("invalid number of parameters");

			//
			// field type
			//
			fieldInfo.type = *begin;

			if(++begin == end)
				throw std::invalid_argument("invalid number of parameters");

			//
			// field size
			//
			fieldInfo.size = lexical_cast<size_t>(*begin);

			//
			// +2 for \r\n
			// 
			totalSize += fieldInfo.size + 2;

			fields.push_back(fieldInfo);

			++begin;
		}

		return make_pair(fields, totalSize);
	}

	struct ProtocolAnswer
	{
		enum Type
		{
			TypeUndefined,
			TypeAnswer,
			TypeEvent
		};

		string rawRequest;

		size_t pendingDataSize;
		Type type;
		string rawAnswerLine;
		string eventName;
		string parameter;
		string parameterType;
		vector<FieldInfo> fieldSet;
		
		bool error;
		string errorMessage;

		TioData key, value, metadata;

		ProtocolAnswer() :
			pendingDataSize(0),
			error(false),
			type(TypeUndefined)
		{}

		void Clear()
		{
			pendingDataSize = 0;
			type = TypeUndefined;
			rawRequest.clear();
			rawAnswerLine.clear();
			eventName.clear();
			parameter.clear();
			parameterType.clear();
			fieldSet.clear();
			error = false;
			errorMessage.clear();
			
			key.Clear();
			value.Clear();
			metadata.Clear();
		}
	};

	inline void ParseAnswerLine(string line, ProtocolAnswer* answer)
	{
		vector<string> params;
		vector<string>::iterator current;
		string answerType, answerLine;

		if(line.empty())
			throw std::invalid_argument("empty command line");
	
		if(*(line.end() - 1) == '\r')
			line.erase(line.end() - 1);

		answer->Clear();

		answer->rawAnswerLine = line;

		boost::split(
			params, 
			line, 
			boost::is_any_of(" "));

		current = params.begin();

		answerType = *current++;

		if(answerType == "answer")
		{
			answer->type = ProtocolAnswer::TypeAnswer;

			const string& result = *current++;

			if(result == "ok")
				answer->error = false;
			else if (result == "error")
				answer->error = true;
			else
				throw std::invalid_argument("invalid line. Result must be \"ok\" or \"error\"");

			if(answer->error)
			{
				//
				// sets the error message
				//
				for( ; current != params.end() ; ++current)
					answer->errorMessage += *current + " ";

				return;
			}
				
			//
			// simple answer, no data
			//
			if(current == params.end())
				return;
			
			answerType = *current++;

			//
			// maybe a final space...
			//
			if(answerType.empty())
				return;

			if(answerType == "data")
			{
				pair_assign(answer->fieldSet, answer->pendingDataSize) = 
					ExtractFieldSet(current, params.end());

				return;
			}
			else if(answerType == "handle" || answerType == "count" || answerType == "type")
			{
				answer->parameterType = answerType;

				if(current == params.end())
					throw std::invalid_argument("invalid answer. dataType specified but no data.");

				answer->parameter = *current++;

				return;
			}
		}
		else if(answerType == "event")
		{
			answer->type = ProtocolAnswer::TypeEvent;

			answer->parameterType = "handle";

			if(current == params.end())
				throw std::runtime_error("invalid answer: no handle value");

			answer->parameter = *current++;

			if(current == params.end())
				throw std::runtime_error("invalid answer: no data");

			answer->eventName = *current++;

			pair_assign(answer->fieldSet, answer->pendingDataSize) = 
				ExtractFieldSet(current, params.end());

			return;
		}
	}
}
