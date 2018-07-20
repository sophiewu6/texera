#include "caf/all.hpp"
#include "caf/io/all.hpp"
#include "./base.hpp"

using namespace std;
using namespace caf;
using namespace rapidjson;

namespace Texera
{
	struct worker_state
	{
		Agent agent_hdl;
		Document json_doc;
	};

	void Execute(Value& o,Document& d)
	{
		if (o["operator"] == "search")
		{
			if(d.IsArray() && d.Size())
			for (Value::ValueIterator i=d.Begin();i!=d.End();)
			{
				string to_search = i->GetString();
				string keyword = o["keyword"].GetString();
				if (to_search.find(keyword) == string::npos)
					i = d.Erase(i);
				else
					++i;
			}
		}
		else if (o["operator"] == "nothing")
		{
			;
		}
		else if (o["operator"] == "sink")
		{
			if (d.IsArray() && d.Size())
				for (Value::ValueIterator i = d.Begin(); i != d.End(); ++i)
					cout << "sink:" << i->GetString() << endl;
		}
	}

	Worker::behavior_type worker_behavior(Worker::stateful_pointer<worker_state> self,actor_addr creator, actor_addr agent)
	{
		self->state.agent_hdl = actor_cast<Agent>(agent);
		self->monitor(self->state.agent_hdl);
		self->set_down_handler([=](down_msg&)
		{
			aout(self) << "agent downs" << endl;
			self->quit();
		});
		return 
		{
			[=](work_atom,int start,int operator_id,const string& work)
			{
				cout << operator_id << work << endl;
				if (!work.empty())
				{
					auto& ops = self->state.json_doc["queue"];
					Document work_json;
					work_json.Parse(work.c_str());
					Execute(ops[operator_id], work_json);
					cout << "output:" << Value2Str(work_json) << endl;
					if (ops[operator_id].HasMember("to"))
						for (auto& i : ops[operator_id]["to"].GetArray())
						{
							int idx = i.GetInt();
							if (ops[idx].HasMember("block") && ops[idx]["block"].GetBool())
								self->send(self->state.agent_hdl, response_atom::value, start, operator_id, Value2Str(work_json));
							else
								self->send(self, work_atom::value, start, idx, Value2Str(work_json));
						}
					else
						self->send(self->state.agent_hdl, request_atom::value);
				}
				else
					self->send(self->state.agent_hdl, request_atom::value);
			},
			[=](init_atom,const string& json)
			{
				self->state.json_doc.Parse(json.c_str());
				return request_atom::value;
			}
		};
	}
}