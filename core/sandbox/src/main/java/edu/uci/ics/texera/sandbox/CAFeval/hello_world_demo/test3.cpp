#include <set>
#include <map>
#include <vector>
#include <cstdlib>
#include <sstream>
#include <iostream>
#include <unordered_map>
#include "caf/all.hpp"
#include "caf/io/all.hpp"

#include "caf/string_algorithms.hpp"


using namespace std;
using namespace caf;


namespace
{
	using msg_atom = atom_constant<atom("message")>;
	using work_atom = atom_constant<atom("work")>;
	using change_atom = atom_constant<atom("change")>;

	class config : public actor_system_config {
	public:
		std::string host = "localhost";
		uint16_t port = 0;
		string name = "Unknown";
		bool server_mode = false;
		config() {
			opt_group{ custom_options_, "global" }
				.add(server_mode, "server,s", "run in server mode")
				.add(host, "host,H", "set host (ignored in server mode)")
				.add(port, "port,p", "set port (ignored in client mode)")
				.add(name, "name,n", "set a name for the node");
		}
	};



	struct client_state
	{
		strong_actor_ptr current_server;
		bool reverse_mode;
		string name;
	};

	

	behavior server_behavior(event_based_actor* self);
	behavior init(stateful_actor<client_state>* self,const string& name);
	behavior disconnected(stateful_actor<client_state>* self);
	behavior connecting(stateful_actor<client_state>* self, const string& host, uint16_t port);
	behavior connected(stateful_actor<client_state>* self, const actor& server);


	behavior init(stateful_actor<client_state>* self,const string& name)
	{
		self->state.reverse_mode = false;
		self->state.current_server = nullptr;
		self->state.name = name;
		self->set_down_handler([=](const down_msg& dm) {
			if (dm.source == self->state.current_server) {
				aout(self) << "*** lost connection to server" << endl;
				self->state.current_server = nullptr;
				self->become(disconnected(self));
			}
		});
		return disconnected(self);
	}

	behavior disconnected(stateful_actor<client_state>* self)
	{
		return
		{
			[=](connect_atom,const string& host,uint16_t port)
		{
			connecting(self,host,port);
		}
		};
	}

	behavior connecting(stateful_actor<client_state>* self, const string& host, uint16_t port)
	{
		self->state.current_server = nullptr;
		auto mm = self->system().middleman().actor_handle();
		self->request(mm, infinite, connect_atom::value, host, port).await(
			[=](const node_id&, strong_actor_ptr serv,
				const std::set<std::string>& ifs) {
			if (!serv) {
				aout(self) << R"(*** no server found at ")" << host << R"(":)"
					<< port << endl;
				return;
			}
			if (!ifs.empty()) {
				aout(self) << R"(*** typed actor found at ")" << host << R"(":)"
					<< port << ", but expected an untyped actor " << endl;
				return;
			}
			aout(self) << "*** successfully connected to server" << endl;
			self->state.current_server = serv;
			auto hdl = actor_cast<actor>(serv);
			self->monitor(hdl);
			self->become(connected(self, hdl));
		},
			[=](const error& err) {
			aout(self) << R"(*** cannot connect to ")" << host << R"(":)"
				<< port << " => " << self->system().render(err) << endl;
			self->become(disconnected(self));
		}
		);
	}

	behavior connected(stateful_actor<client_state>* self, const actor& server)
	{
		auto id = self->id();
		self->request(server, infinite, connect_atom::value, self->address())
			.await([=]() {aout(self) << "Successfully Registed itself." << endl; });
		return
		{
			[=](msg_atom,const string& msg)
		{
			stringstream t;
			t << "Worker " <<self->state.name << " replies: ";
			if (self->state.reverse_mode)
				t << string(msg.rbegin(), msg.rend());
			else
				t << msg;
			auto hdl = actor_cast<actor>(self->state.current_server);
			self->send(hdl,t.str());
		},
			[=](work_atom,const string& work,int param)
		{
			aout(self) << "Worker " << self->state.name<< " get: " << work << endl;
			stringstream t;
			t << "Worker " << self->state.name << " replies: ";
			if (work == "sleep")
			{
				this_thread::sleep_for(chrono::seconds(param));
				t << "Finished sleeping for " << param << "s";
			}
			else if (work == "reverse_mode")
			{
				self->state.reverse_mode = (bool)param;
				t << "Current mode: " << (self->state.reverse_mode ? "reverse" : "echo");
			}
			else
			{
				t << "Unknown Command. Trying to interpret as message...";
				self->send(self, msg_atom::value, work);
			}
			return t.str();
		}
		};
	}

	string trim(std::string s) {
		auto not_space = [](char c) { return isspace(c) == 0; };
		// trim left
		s.erase(s.begin(), find_if(s.begin(), s.end(), not_space));
		// trim right
		s.erase(find_if(s.rbegin(), s.rend(), not_space).base(), s.end());
		return s;
	}


	pair<string, int> parse_work(const string& work)
	{
		pair<string, int> res("", 0);
		istringstream iss(work);
		vector<string> results(istream_iterator<string>{iss}, istream_iterator<string>());
		if (results.size() > 1)
		{
			res.first = results[0];
			try { res.second=stoi(results[1]); }
			catch (...) { res.first = work; };
		}
		else
			res.first = work;
		return res;
	}


	behavior server_behavior(event_based_actor* self)
	{
		auto workers = actor_pool::make(self->context(), actor_pool::round_robin());
		return
		{
			[=](connect_atom,actor_addr worker)
		{
			auto hdl = actor_cast<actor>(worker);
			aout(self) << "addr: " << worker << " Connected!" << endl;
			anon_send(workers, sys_atom::value, put_atom::value, hdl);
		},
			[=](const string& msg)
		{
			aout(self) << msg << endl;
		},
			[=](work_atom,const string& work)
		{
			aout(self) << "Sending work to workers using policies" << endl;
			aout(self) << "Work content: " << work << endl;
			auto res = parse_work(work);
			anon_send(workers, work_atom::value, res.first, res.second);
		}
		};
	}
	void run_server(actor_system& system, const config& cfg)
	{
		auto serv = system.spawn(server_behavior);
		// try to publish math actor at given port
		cout << "*** try publish at port " << cfg.port << endl;
		auto expected_port = io::publish(serv, cfg.port);
		if (!expected_port) {
			std::cerr << "*** publish failed: "
				<< system.render(expected_port.error()) << endl;
			return;
		}
		cout << "*** server successfully published at port " << *expected_port << endl;
		while (1)
		{
			string input;
			std::getline(std::cin, input);
			if (input == "quit")
			{
				anon_send_exit(serv, exit_reason::user_shutdown);
				return;
			}
			else
			{
				string::size_type sz = 0;
				input = trim(input.substr(sz));
				anon_send(serv, work_atom::value, input);
			}
		}
	}

	void run_client(actor_system& system, const config& cfg)
	{
		auto client = system.spawn(init, cfg.name);
		if (!cfg.host.empty() && cfg.port > 0)
			anon_send(client, connect_atom::value, cfg.host, cfg.port);
		else
		{
			cout << "*** no server received via config, " << endl;
			anon_send_exit(client, exit_reason::user_shutdown);
			return;
		}
		cout << "Use [enter] to shutdown..." << endl;
		string dummy;
		std::getline(std::cin, dummy);
		anon_send_exit(client, exit_reason::user_shutdown);
	}



	void caf_main(actor_system& system, const config& cfg)
	{
		auto f = cfg.server_mode ? run_server : run_client;
		f(system, cfg);
	}

}

CAF_MAIN(io::middleman)