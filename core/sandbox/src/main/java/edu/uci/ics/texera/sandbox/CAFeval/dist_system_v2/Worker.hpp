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
		vector<vector<string>> data;
		int start_from;
		bool need_pause;
		int resume_idx;
	};

	void Execute(Value& o,vector<vector<string>>& d)
	{
		string op = o["operator"].GetString();
		if (op == "search")
		{
			string keyword = o["keyword"].GetString();
			int idx = o["field"].GetInt();
			for (auto i=d.begin();i!=d.end();)
			{
				auto& to_search = *i;
				if (to_search.size()<=idx || to_search[idx].find(keyword) == string::npos)
					i = d.erase(i);
				else
					++i;
			}
			d.shrink_to_fit();
		}
		else if (op == "filter")
		{
			auto idxs = o["field"].GetArray();
			for (auto i = d.begin(); i != d.end();++i)
			{
				vector<string> temp;
				for (auto& j : idxs)
					temp.push_back((*i)[j.GetInt()]);
				(*i).swap(temp);
			}
		}
		else if (op == "sink")
		{
			for (auto i : d)
			{
				cout << " sink:";
				for (auto j : i)
					cout << j <<" ";
				cout << endl;
			}
		}
	}

	Worker::behavior_type worker_behavior(Worker::stateful_pointer<worker_state> self,actor_addr creator, string hostname,uint16_t port)
	{
		auto agent = actor_cast<Agent>(*(self->system().middleman().remote_actor(hostname, port)));
		self->send(agent, register_atom::value, self->address());
		self->state.agent_hdl = agent;
		self->set_down_handler([=](down_msg&)
		{
			aout(self) << "agent downs" << endl;
			self->quit();
		});
		return 
		{
			[=](work_atom,int start,vector<vector<string>>& input)
			{
				if (start == -1) 
				{
					self->send(self->state.agent_hdl, request_atom::value,-1); 
					return;
				}
				self->state.start_from = start;
				self->state.data = move(input);
				if (self->state.need_pause)
					self->state.resume_idx = start;
				else
					self->send(self, continue_atom::value, start);
			},
			[=](continue_atom,int idx)
			{
				if (self->state.need_pause)
				{
					self->state.resume_idx = idx;
					return;
				}
				if (!self->state.data.empty())
				{
					auto& ops = self->state.json_doc["queue"];
					Execute(ops[idx],self->state.data);
					if (ops[idx].HasMember("to"))
						for (auto& i : ops[idx]["to"].GetArray())
						{
							int to_idx = i.GetInt();
							if (ops[to_idx].HasMember("block") && ops[to_idx]["block"].GetBool())
								self->send(self->state.agent_hdl, response_atom::value, self->state.start_from, idx, self->state.data);
							else
								self->send(self, continue_atom::value, to_idx);
						}
					else
						self->send(self->state.agent_hdl, request_atom::value, self->state.start_from);
				}
				else
					self->send(self->state.agent_hdl, request_atom::value, self->state.start_from);
			},
			[=](init_atom,const string& json)
			{
				self->state.need_pause = false;
				self->state.json_doc.Parse(json.c_str());
				return result<request_atom,int>(request_atom::value, -1);
			},
			[=](pause_atom) 
			{
				self->state.need_pause = true;
			},
			[=](resume_atom)
			{
				self->state.need_pause = false;
				self->send(self, continue_atom::value, self->state.resume_idx);
			}
		};
	}
}