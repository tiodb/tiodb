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
	using std::vector;
	using std::set;
	using std::map;

class Auth : public boost::noncopyable
{
public:
	enum RuleResult
	{
		allow, deny, none
	};

private:
	struct COMMAND
	{
		string name;

		typedef set<string> Tokens;

		Tokens denies;
		Tokens allows;

		RuleResult defaultRule;

		bool operator < (const COMMAND& command)
		{
			return name < command.name;
		}
	};

	struct OBJECT
	{
		string name;
		RuleResult defaultRule;
		
		typedef map<string, COMMAND> CommandMap;
		CommandMap commands;
	};

	typedef map<string, OBJECT> ObjectRules;
	
	ObjectRules objectRules_;
	RuleResult objectDefaultRule_;

	typedef map<string, COMMAND> CommandRules;
	CommandRules commandRules_;
	RuleResult commandDefaultRule_;

	bool FindRuleForTokens(const COMMAND::Tokens& commandTokens, const vector<string>& tokens)
	{	
		BOOST_FOREACH(const string& token, tokens)
		{
			if(commandTokens.find(token) != commandTokens.end())
				return true;
		}
		return false;
	}

	RuleResult CheckCommandAccess(const COMMAND& cmd, const vector<string>& tokens)
	{
		if(FindRuleForTokens(cmd.denies, tokens))
			return deny;

		if(FindRuleForTokens(cmd.allows, tokens))
			return allow;

		return cmd.defaultRule;
	}

public:

	Auth()
	{
		objectDefaultRule_ = allow;
		commandDefaultRule_ = allow;
	}

	void AddObjectRule(const string& objectType, const string& objectName, 
		const string& command, const string& token, RuleResult RuleResult)
	{
		string fullQualifiedName = objectType + "/" + objectName;

		COMMAND& cmd = objectRules_[fullQualifiedName].commands[command];

		if(RuleResult == allow)
			cmd.allows.insert(token);
		else if(RuleResult == deny)
			cmd.denies.insert(token);
	}

	void SetObjectDefaultRule(const string& objectType, const string& objectName, RuleResult defaultRule)
	{
		string fullQualifiedName = objectType + "/" + objectName;

		OBJECT& obj = objectRules_[fullQualifiedName];

		obj.defaultRule = defaultRule;
	}

	void SetDefaultRule(RuleResult defaultRule)
	{
		objectDefaultRule_ = defaultRule;
	}

	RuleResult CheckCommandAccess(const string& command, const vector<string>& tokens)
	{		
		CommandRules::const_iterator i = commandRules_.find(command);

		if(i == commandRules_.end())
			return commandDefaultRule_;

		return CheckCommandAccess(i->second, tokens);
	}

	
	RuleResult CheckObjectAccess(const string& objectType, const string& objectName, 
		const string& command, const string& token)
	{
		vector<string> tokens;
		tokens.push_back(token);
		
		return CheckObjectAccess(objectType, objectName, command, tokens);
	}

	RuleResult CheckObjectAccess(const string& objectType, const string& objectName, 
		const string& command, const vector<string>& tokens)
	{
		string fullQualifiedName = objectType + "/" + objectName;

		//
		// object
		//
		ObjectRules::const_iterator iobj = objectRules_.find(fullQualifiedName);

		if(iobj == objectRules_.end())
			return objectDefaultRule_;

		//
		// commands
		//
		const OBJECT& obj = iobj->second;
		OBJECT::CommandMap::const_iterator icmd;

		//
		// we support "*" for commands
		//
		if(obj.commands.size() == 1 && obj.commands.begin()->first == "*")
			icmd = obj.commands.begin();
		else
			icmd = obj.commands.find(command);
		
		if(icmd == obj.commands.end())
			return obj.defaultRule;

		// 
		// Rules
		//
		const COMMAND& cmd = icmd->second;

		return CheckCommandAccess(cmd, tokens);		
	}
};

}