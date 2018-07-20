#include "caf/all.hpp"
#include "caf/io/all.hpp"
#include "./Worker.hpp"
#define MAX_WORKER_PER_AGENT 2
#define BATCH_SIZE 10
using namespace std;
using namespace caf;
using namespace rapidjson;

namespace Texera
{


	struct agent_state
	{
		Document json_doc;
		unordered_set<Worker> workers;
		unordered_set<Worker> idle_workers;
		unordered_map<int, ifstream> inputs;
		unordered_map<int, vector<string>> pending_res;
		unordered_map<int, int> still_working;
		bool flush;
	};



	void Parse(Agent::stateful_pointer<agent_state> self,const string& task)
	{
		self->state.json_doc.Parse(task.c_str());
		self->state.flush = false;
		const Document& d = self->state.json_doc;
		assert(d["queue"].IsArray());
		const Value& q = d["queue"];
		for (SizeType i = 0; i < q.Size(); i++)
			if (!q[i].HasMember("from"))
			{
				self->state.inputs[i] = ifstream(q[i]["filename"].GetString());
			}
	}

	pair<int,string> SelectWork(Agent::stateful_pointer<agent_state> self)
	{

		Value& q=self->state.json_doc["queue"];
		auto& inputs = self->state.inputs;
		auto& pending_res = self->state.pending_res;
		auto& still_working = self->state.still_working;
		Document::AllocatorType& allocator = self->state.json_doc.GetAllocator();
		int c = 0;
		for (Value& v : q.GetArray())
		{
			if (v.HasMember("from"))
			{
				auto arr = v["from"].GetArray();
				auto id = v["id"].GetInt();
				auto cond1 = (v.HasMember("block") && v["block"].GetBool() && all_of(arr.Begin(), arr.End(), [&](const Value& i)
				{
					for (auto i : still_working)
						if (i.first < id && i.second)
							return false;
					int idx = i.GetInt();
					return (pending_res.find(idx) != pending_res.end() && !pending_res[idx].empty())
						|| (inputs.find(idx) != inputs.end() && inputs[idx].good());
				}));
				auto cond2 = ((!v.HasMember("block") || !v["block"].GetBool()) && all_of(arr.Begin(), arr.End(), [&](const Value& i)
				{
					int idx = i.GetInt();
					return (pending_res.find(idx) != pending_res.end() && !pending_res[idx].empty())
						|| (inputs.find(idx) != inputs.end() && inputs[idx].good());
				}));
				if (cond1)self->state.flush = true;
				if (cond1 || cond2)
				{

					Value res(kArrayType);
					bool isgood = true;
					for (int i = 0; i < BATCH_SIZE; ++i)
					{
						for (auto& j : arr)
						{
							int idx = j.GetInt();
							if (inputs.find(idx) != inputs.end())
							{
								if (!inputs[idx].good()) { isgood = false; break; }
								string str;
								inputs[idx] >> str;
								res.PushBack(Value(str.c_str(), allocator).Move(), allocator);
							}
							else
							{
								if (pending_res[idx].empty()) { isgood = false; break; }
								res.PushBack(Value(pending_res[idx].back().c_str(), allocator).Move(), allocator);
								pending_res[idx].pop_back();
							}
						}
						if (!isgood)
							break;
					}
					return { v["id"].GetInt(),Value2Str(res) };
				}

			}
		}
		return { -1,"" };
	}

	Agent::behavior_type agent_behavior(Agent::stateful_pointer<agent_state> self)
	{
		return 
		{
			[=](register_atom, actor_addr addr)
			{
				auto worker = actor_cast<Worker>(addr);
				self->monitor(worker);
				self->state.workers.insert(worker);
			},
			[=](work_atom,const string& task)
			{
				Parse(self, task);
				for (auto i : self->state.workers)
					self->send(i, init_atom::value,task);
			},
			[=](response_atom,int start,int idx,const string& response)
			{
				self->state.still_working[start]--;
				Document d;
				d.Parse(response.c_str());
				for(auto& i:d.GetArray())
					self->state.pending_res[idx].push_back(i.GetString());
				auto next_work = SelectWork(self);
				if (self->state.flush)
				{
					aout(self) << "enter flush!" << endl;
					for (auto i : self->state.idle_workers)
					{
						auto work = SelectWork(self);
						if (work.first == -1)break;
						self->send(i, work_atom::value, work.first, work.first, work.second);
					}
					self->state.flush = false;
				}
				if (next_work.first==-1)
				{
					auto worker = actor_cast<Worker>(self->current_sender());
					self->state.idle_workers.insert(worker);
					self->state.workers.erase(worker);
				}
				if (self->state.workers.empty())
				{
					for (auto i : self->state.still_working)
					{
						aout(self) << i.first << " " << i.second << endl;
					}
					aout(self) << "Work finished!" << endl;
					for (auto& worker : self->state.idle_workers)
					{
						anon_send_exit(worker, exit_reason::kill);
					}
					self->quit(exit_reason::normal);
				}
				if(next_work.first!=-1)
					self->state.still_working[next_work.first]++;
				return make_tuple(work_atom::value, next_work.first, next_work.first,next_work.second);
			},
			[=](request_atom)
			{
				auto next_work = SelectWork(self);
				if (next_work.first==-1)
				{
					auto worker = actor_cast<Worker>(self->current_sender());
					self->state.idle_workers.insert(worker);
					self->state.workers.erase(worker);
				}
				if (self->state.workers.empty())
				{
					aout(self) << "Work finished!" << endl;
					for (auto& worker : self->state.idle_workers)
					{
						self->send_exit(worker, exit_reason::kill);
					}
					self->quit(exit_reason::normal);
				}
				if(next_work.first!=-1)
					self->state.still_working[next_work.first]++;
				return make_tuple(work_atom::value,next_work.first, next_work.first,next_work.second);
			}
		};
	}
}