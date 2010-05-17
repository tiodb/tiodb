#pragma once
#include "buffer.h"


namespace tio
{
	using namespace std;
	using boost::shared_ptr;

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
