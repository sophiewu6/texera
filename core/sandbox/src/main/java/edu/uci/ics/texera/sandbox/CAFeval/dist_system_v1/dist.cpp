#include <vector>
#include <cstdlib>
#include <sstream>
#include <iostream>
#include <unistd.h>
#include <limits.h>
#include <string>
#include "caf/all.hpp"
#include "caf/io/all.hpp"
#include "caf/string_algorithms.hpp"
#include "./Manager.hpp"

char hostname[HOST_NAME_MAX];

using namespace std;
using namespace caf;
using namespace Texera;


namespace 
{

	class config : public actor_system_config {
	public:
		std::string host = "localhost";
		uint16_t port = 0, hostport = 0;
		bool is_manager = false;
		config() {
			add_actor_type(metadata<Worker>::class_str, worker_behavior);
			add_actor_type(metadata<Agent>::class_str, agent_behavior);
			opt_group{ custom_options_, "global" }
				.add(is_manager, "server,s", "run in manager machines")
				.add(host, "host,H", "set host (ignored in manager machines)")
				.add(hostport, "hostport,r", "set host port")
				.add(port, "port,p", "set port (ignored in manager machines)");

		}
	};


	void run_worker(actor_system& system, const config& cfg)
	{
		auto res = system.middleman().open(cfg.port);
		if (!res) 
		{
			cerr << "*** cannot open port: "
				<< system.render(res.error()) << endl;
			return;
		}
		auto manager = system.middleman().remote_actor(cfg.host, cfg.hostport);
		if (!manager)
			cerr << "unable to connect to manager: " << system.render(manager.error()) << std::endl;
		else
			anon_send(*manager, connect_atom::value, string(hostname), cfg.port);
			
		cout << "*** running on port: "
			<< *res << endl
			<< "*** press <enter> to shutdown" << endl;
		getchar();
		anon_send(*manager, disconnect_atom::value, string(hostname), cfg.port);

	}

	void run_manager(actor_system& system, const config& cfg)
	{
		auto manager = system.spawn(Manager);
		auto res=system.middleman().publish(manager, cfg.port);
		if (!res)
		{
			cerr << "*** cannot publish manager: "
				<< system.render(res.error()) << endl;
			return;
		}
		cout << "*** published manager on port " << *res << endl;
		cout << "*** enter 'quit' to shutdown" << endl;
		while (1)
		{
			string input;
			getline(cin,input);
			if (input == "quit")
			{
				anon_send_exit(manager, exit_reason::kill);
				return;
			}
			anon_send(manager, work_atom::value, input);
		}
	}


	void caf_main(actor_system& system, const config& cfg)
	{
		gethostname(hostname, HOST_NAME_MAX);
		auto f = cfg.is_manager ? run_manager : run_worker;
		f(system, cfg);
	}

}

CAF_MAIN(io::middleman)
