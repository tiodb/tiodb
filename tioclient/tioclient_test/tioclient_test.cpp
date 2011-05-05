#include <string>
#include <assert.h>
#include <iostream>
#include <sstream>
#include "../tioclient.hpp"

using std::string;

namespace XPTO
{
	struct TEST_STRUCT
	{
		string s;
		int i;
	};

	void ToTioData(const TEST_STRUCT& value, TIO_DATA* tiodata)
	{
		std::stringstream str;

		str << value.s << ";" << value.i;

		tiodata_set_string(tiodata, str.str().c_str());
	}
}


void test_structs()
{
	tio::Connection cn;

	cn.Connect("127.0.0.1", 6666);

	tio::containers::list<XPTO::TEST_STRUCT> l;
	XPTO::TEST_STRUCT t;

	l.create(&cn, "xpto", "volatile_list");

	t.i = 10;
	t.s = "test";

	l.push_back(t);

}

extern "C" 
{
	void test_cpp_client()
	{
		tio::Connection cn;
		tio::containers::list<string> l;

		cn.Connect("127.0.0.1", 6666);

		l.create(&cn, "test_list", "volatile_list");
		l.clear();

		l.push_back("0");
		assert(l[0] == "0");

		l.push_back("1");
		assert(l[l.size()-1] == "1");

		l[0] = "xpto";
		assert(l[0] == "xpto");

		assert(l.pop_back() == "1");
		assert(l.pop_front() == "xpto");

		tio::containers::map<string, string> m;

		m.create(&cn, "test_map", "volatile_map");

		m["a"] = "b";
		assert(m["a"] == "b");


		return;
	}
}

int main()
{
	test_cpp_client();
	test_structs();
}