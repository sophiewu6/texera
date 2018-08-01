#include <vector>
#include <map>
#include <algorithm>
#include <vector>
#include <cstdlib>
#include <sstream>
#include <iostream>
#include <unistd.h>
#include <limits.h>
#include <string>
#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/filereadstream.h"
#include "rapidjson/prettywriter.h"
#include "caf/all.hpp"
#include "caf/io/all.hpp"

#define EMPTY_LAMBDA [](){}
char hostname[HOST_NAME_MAX];

using namespace std;
using namespace caf;
using namespace rapidjson;

namespace Texera
{

	//communication types
	using msg_atom = atom_constant<atom("message")>;
	using on_start = atom_constant<atom("on_start")>;
	using on_finish = atom_constant<atom("on_finish")>;
	using on_work = atom_constant<atom("on_work")>;
	using disconnect_atom = atom_constant<atom("disconnect")>;

	

	template <class Inspector>
	typename Inspector::result_type inspect(Inspector& f, vector<vector<string>>& t) {
		return f(meta::type_name("vector<vector<string>>"),t);
	}

	//code from:
	//https://stackoverflow.com/questions/236129/the-most-elegant-way-to-iterate-the-words-of-a-string?noredirect=1&lq=1

	template<typename Out>
	void split(const std::string &s, char delim, Out result) {
		std::stringstream ss(s);
		std::string item;
		while (std::getline(ss, item, delim)) {
			*(result++) = item;
		}
	}

	std::vector<std::string> split(const std::string &s, char delim) {
		std::vector<std::string> elems;
		split(s, delim, std::back_inserter(elems));
		return elems;
	}




	string Value2Str(const Value& v)
	{
		StringBuffer sb;
		Writer<StringBuffer> writer(sb);
		v.Accept(writer);
		return sb.GetString();
	}

}