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
		vector<pair<int,vector<vector<string>>>> pending_work;
		bool need_pause;
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
					temp.emplace_back((*i)[j.GetInt()]);
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
		self->monitor(agent);
		self->send(agent, register_atom::value, self->address());
		self->state.agent_hdl = agent;
		self->set_down_handler([=](down_msg&)
		{
			aout(self) << "agent downs" << endl;
			self->quit();
		});
		return 
		{
			/*
			[=](work_atom,int start,vector<vector<string>>& input)
			{
				//aout(self) << "received input" << endl;
				if (start == -1)  
					return;
				if (self->state.need_pause)
					;
				else
					self->send(self, continue_atom::value, start);
			},
			*/
			[=](work_atom,int idx,vector<vector<string>>& input)
			{
				//aout(self) << "continue :" << idx << endl;
				if (idx == -1)return;
				if (self->state.need_pause)
				{
					self->state.pending_work.emplace_back(make_pair(idx, move(input)));
					return;
				}
				if (!input.empty())
				{
					self->send(self->state.agent_hdl, update_atom::value, idx, true);
					auto& ops = self->state.json_doc["queue"];
					Execute(ops[idx], input);
					self->send(self->state.agent_hdl, update_atom::value, idx, false);
					if (ops[idx].HasMember("to"))
					{
						for (Value& i : ops[idx]["to"].GetArray())
						{
							int to_idx = i.GetInt();
							if (ops[to_idx].HasMember("block") && ops[to_idx]["block"].GetBool())
							{
								if (i == *(ops[idx]["to"].End()))
									self->send(self->state.agent_hdl, response_atom::value, idx, move(input));
								else
									self->send(self->state.agent_hdl, response_atom::value, idx, input);
							}
							else
							{
								if (i == *(ops[idx]["to"].End()))
									self->send(self, work_atom::value, to_idx, move(input));
								else
									self->send(self, work_atom::value, to_idx, input);
							}
						}
					}
					else
						self->send(self->state.agent_hdl, request_atom::value);
				}
				else
					self->send(self->state.agent_hdl, request_atom::value);
			},
			[=](init_atom,const string& json)
			{
				self->state.need_pause = false;
				self->state.json_doc.Parse(json.c_str());
				return result<request_atom>(request_atom::value);
			},
			[=](pause_atom) 
			{
				self->state.need_pause = true;
			},
			[=](resume_atom)
			{
				self->state.need_pause = false;
				while (!self->state.pending_work.empty())
				{
				    auto temp = self->state.pending_work.back();
					self->send(self, work_atom::value, temp.first,move(temp.second));
					self->state.pending_work.pop_back();
				}
			}
		}; 
	}
}