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
	using work_atom = atom_constant<atom("work")>;
	using continue_atom = atom_constant<atom("cont")>;
	using register_atom = atom_constant<atom("register")>;
	using response_atom = atom_constant<atom("response")>;
	using init_atom = atom_constant<atom("init")>;
	using request_atom = atom_constant<atom("request")>;
	using disconnect_atom = atom_constant<atom("disconnect")>;
	using pause_atom = atom_constant<atom("pause")>;
	using resume_atom = atom_constant<atom("resume")>;

	//metadata for actors
	template<typename T>
	struct metadata { static constexpr const char* class_str = "Unknown"; };
	
	using Worker = typed_actor <
		reacts_to<work_atom, int, vector<vector<string>>>,
		replies_to<init_atom, string>::with<request_atom>,
		reacts_to<pause_atom>,
		reacts_to<resume_atom>
	>;

	template<>
	struct metadata<Worker>
	{
		static constexpr const char* class_str = "Worker";
	};

	using Agent = typed_actor<
		reacts_to<work_atom, string>,
		replies_to<register_atom, actor_addr>::with<init_atom,string>,
		replies_to<response_atom, int, vector<vector<string>>>::with<work_atom, int,vector<vector<string>>>,
		replies_to<request_atom>::with<work_atom, int, vector<vector<string>>>,
		reacts_to<pause_atom>,
		reacts_to<resume_atom>,
		reacts_to<pause_atom,Worker>,
		reacts_to<resume_atom,Worker>,
		reacts_to<update_atom,int,bool>,
		replies_to<publish_atom,int,int>::with<continue_atom, int, string, uint16_t>
	>;

	template<>
	struct metadata<Agent>
	{
		static constexpr const char* class_str = "Agent";
	};


	template <class Inspector>
	typename Inspector::result_type inspect(Inspector& f, vector<vector<string>>& t) {
		return f(meta::type_name("vector<vector<string>>"),t);
	}


	string Value2Str(const Value& v)
	{
		StringBuffer sb;
		Writer<StringBuffer> writer(sb);
		v.Accept(writer);
		return sb.GetString();
	}

}