#include "caf/all.hpp"
#include "caf/io/all.hpp"
#include "./Worker.hpp"


using namespace std;
using namespace caf;

namespace Texera
{
	using time_point = std::chrono::high_resolution_clock::time_point;

	struct manager_state
	{
		vector<pair<string,uint16_t>> hosts;
		time_point start;
		int current_index;
		int cursor;
	};

	template<typename T>
	string CreateActor(stateful_actor<manager_state>* self,const message& args=make_message(), std::chrono::nanoseconds tout=chrono::milliseconds(200))
	{
		self->state.cursor %= self->state.hosts.size();
		auto hp = self->state.hosts[self->state.cursor++];
		auto node = self->system().middleman().connect(hp.first, hp.second);
		if (!node)
			return hp.first;
		self->system().middleman().remote_spawn<T>(*node, T::class_str, args, tout);
		return hp.first;
	}


	behavior Manager(stateful_actor<manager_state>* self)
	{
		self->state.current_index = 1;
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
			[=](on_work,int batch_size)
			{
				if (batch_size < 1)
				{
					aout(self) << "invaild batch_size" << endl;
					return;
				}
				self->state.cursor %= self->state.hosts.size();
				auto hp1 = self->state.hosts[self->state.cursor++];
				auto mg1 = self->system().middleman().remote_actor(hp1.first, hp1.second);
				self->send(*mg1, "Sink", self->address());
				self->state.cursor %= self->state.hosts.size();
				auto hp2 = self->state.hosts[self->state.cursor++];
				auto mg2 = self->system().middleman().remote_actor(hp2.first, hp2.second);
				self->send(*mg2, "Filter", hp1.first, vector<int>{6, 13});
				self->state.cursor %= self->state.hosts.size();
				auto hp3 = self->state.hosts[self->state.cursor++];
				auto mg3 = self->system().middleman().remote_actor(hp3.first, hp3.second);
				self->send(*mg3, "Keyword", hp2.first, 1,"China");
				self->state.cursor %= self->state.hosts.size();
				auto hp4 = self->state.hosts[self->state.cursor++];
				auto mg4 = self->system().middleman().remote_actor(hp4.first, hp4.second);
				self->send(*mg4, "Scan", hp3.first, batch_size,"large_input.csv");
				//auto sink=CreateActor<Sink>(self, make_message());
				//auto filter=CreateActor<Filter>(self, make_message(sink, vector<int>{6,13}));
				//auto keyword=CreateActor<Keyword>(self, make_message(filter,"China"));
				//CreateActor<Scan>(self, make_message(keyword, batch_size, "large_input.csv"));
				self->state.start = chrono::high_resolution_clock::now();
			},
			[=](on_finish)
			{
				auto end= chrono::high_resolution_clock::now();
				auto begin = self->state.start;
				aout(self) << "time usage: " << (std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count() / 1000.0) << " s" << endl;
			}
		};
	}
}