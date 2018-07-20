#include <vector>
#include <map>
#include <algorithm>
#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/filereadstream.h"
#include "rapidjson/prettywriter.h"
#include "caf/all.hpp"
#include "caf/io/all.hpp"

using namespace std;
using namespace caf;
using namespace rapidjson;

namespace Texera
{
	//communication types
	using msg_atom = atom_constant<atom("message")>;
	using work_atom = atom_constant<atom("work")>;
	using register_atom = atom_constant<atom("register")>;
	using response_atom = atom_constant<atom("response")>;
	using init_atom = atom_constant<atom("init")>;
	using request_atom = atom_constant<atom("request")>;
	using disconnect_atom = atom_constant<atom("disconnect")>;


	//metadata for actors
	template<typename T>
	struct metadata { static constexpr const char* class_str = "Unknown"; };
	
	using Worker = typed_actor <
		reacts_to<work_atom, int, int, string>,
		replies_to<init_atom, string>::with<request_atom>
	>;

	template<>
	struct metadata<Worker>
	{
		static constexpr const char* class_str = "Worker";
	};

	using Agent = typed_actor<
		reacts_to<work_atom, string>,
		reacts_to<register_atom, actor_addr>,
		replies_to<response_atom, int, int, string>::with<work_atom, int, int, string>,
		replies_to<request_atom>::with<work_atom, int, int, string>
	>;

	template<>
	struct metadata<Agent>
	{
		static constexpr const char* class_str = "Agent";
	};



	string Value2Str(const Value& v)
	{
		StringBuffer sb;
		Writer<StringBuffer> writer(sb);
		v.Accept(writer);
		return sb.GetString();
	}

}