#include "caf/all.hpp"
#include "caf/io/all.hpp"
#include "./Agent.hpp"


using namespace std;
using namespace caf;

namespace Texera
{
	using time_point = std::chrono::high_resolution_clock::time_point;

	struct manager_state
	{
		vector<pair<string,uint16_t>> hosts;
		unordered_map<int,Agent> running_agents;
		unordered_map<Agent, time_point> agents_start;
		int current_index;
		int cursor;
	};

	template<typename T>
	expected<T> CreateActor(stateful_actor<manager_state>* self,const message& args=make_message(), std::chrono::nanoseconds tout=chrono::milliseconds(200))
	{
		self->state.cursor %= self->state.hosts.size();
		auto hp = self->state.hosts[self->state.cursor++];
		auto node = self->system().middleman().connect(hp.first, hp.second);
		if (!node)
			return node.error();
		return self->system().middleman().remote_spawn<T>(*node, metadata<T>::class_str, args, tout);
	}


	behavior Manager(stateful_actor<manager_state>* self)
	{
		self->state.current_index = 1;
		self->set_down_handler([=](down_msg& msg)
		{
			auto end = chrono::high_resolution_clock::now();
			auto agent = actor_cast<Agent>(msg.source);
			aout(self) << "Agent(address:" << msg.source << ") finished work!" << endl;
			if (self->state.agents_start.find(agent) != self->state.agents_start.end())
			{
				auto begin = self->state.agents_start[agent];
				aout(self) <<"time usage: "<<(std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count() / 1000.0)<<" s" << endl;
				self->state.agents_start.erase(agent);
			}
			else
				aout(self) << "time usage cannot be measured" << endl;
		});


		return
		{
			[=](connect_atom, const string& host,uint16_t port)
			{
				self->state.hosts.emplace_back(make_pair(host,port));
				aout(self) << "host: " << host <<" port: "<<port<< " Connected!" << endl;
			},
			[=](disconnect_atom, const string& host,uint16_t port)
			{
				auto& vec = self->state.hosts;
				vec.erase(std::remove(vec.begin(), vec.end(), make_pair(host,port)),vec.end());
				aout(self) << "host: " << host << " port: " << port << " Disonnected!" << endl;
			},
			[=](msg_atom, const string& msg)
			{
				aout(self) << msg << endl;
			},
			[=](pause_atom,int idx)
			{
				auto& running_agents = self->state.running_agents;
				if (running_agents.find(idx)!=running_agents.end())
					self->send(running_agents[idx], pause_atom::value);
				else
					aout(self) << "invaild Agent index" << endl;
			},
			[=](resume_atom,int idx)
			{
				auto& running_agents = self->state.running_agents;
				if (running_agents.find(idx) != running_agents.end())
					self->send(running_agents[idx], resume_atom::value);
				else
					aout(self) << "invaild Agent index" << endl;
			},		
			[=](work_atom,const string& task, int num_workers,int batch_size)
			{
				ifstream in(task);
				if (!in.good())
				{
					aout(self) << "invaild json file" << endl;
					return;
				}
				if (num_workers < 1)
				{
					aout(self) << "invaild num_workers" << endl;
					return;
				}
				if (batch_size < 1)
				{
					aout(self) << "invaild batch_size" << endl;
					return;
				}

				//create one agent
				auto agent = CreateActor<Agent>(self);
				if (!agent)
				{
					aout(self) << "*** agent remote spawn failed: "
						<< self->system().render(agent.error()) << endl;
				}
				else
				{
					self->send(*agent, publish_atom::value,num_workers,batch_size);
					self->monitor(*agent);
					std::string file_content(static_cast<std::stringstream const&>(std::stringstream() << in.rdbuf()).str());
					in.close();
					self->send(*agent, work_atom::value, file_content);
					self->state.running_agents.emplace(self->state.current_index, *agent);
					self->state.agents_start.emplace(*agent, chrono::high_resolution_clock::now());
					aout(self) << "Agent(address:"<<(*agent)->address()<<") initialized!"<<endl;
					aout(self) << "recorded at index " << self->state.current_index++ << endl;
				}
			},
			[=](continue_atom,int num_workers,string hostname,uint16_t port)
			{
				//spawn num_workers workers
				for (int i = 0; i<num_workers; ++i)
				{
					auto worker = CreateActor<Worker>(self, make_message(self->address(),hostname,port));
					if (!worker)
					{
						aout(self) << "*** worker remote spawn failed: "
							<< self->system().render(worker.error()) << endl;
					}

				}
			}
		};
	}
}