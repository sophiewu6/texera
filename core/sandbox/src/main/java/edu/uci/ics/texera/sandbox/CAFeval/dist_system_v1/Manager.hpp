#include "caf/all.hpp"
#include "caf/io/all.hpp"
#include "./Agent.hpp"


using namespace std;
using namespace caf;

namespace Texera
{

	struct manager_state
	{
		vector<pair<string,uint16_t>> hosts;
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
		self->set_down_handler([=](down_msg& msg)
		{

		});


		return
		{
			[=](connect_atom, const string& host,uint16_t port)
			{
				self->state.hosts.push_back(make_pair(host,port));
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
			[=](work_atom,const string& task)
			{
				//create one agent
				auto agent = CreateActor<Agent>(self);
				if (!agent)
				{
					cerr << "*** agent remote spawn failed: "
						<< self->system().render(agent.error()) << endl;
				}
				else
				{
					self->monitor(*agent);
				}
				//spawn MAX_WORKER_PER_AGENT workers
				for (int i=0;i<MAX_WORKER_PER_AGENT;++i)
				{
					auto worker = CreateActor<Worker>(self, make_message(self->address(), agent->address()));
					if (!worker)
					{
						cerr << "*** worker remote spawn failed: "
							<< self->system().render(worker.error()) << endl;
					}
					else
					{
						//register all workers
						self->send(*agent, register_atom::value, worker->address());
					}

				}
				ifstream in(task);
				std::string file_content(static_cast<std::stringstream const&>(std::stringstream() << in.rdbuf()).str());
				in.close();
				self->send(*agent, work_atom::value, file_content);
			}
		};
	}
}