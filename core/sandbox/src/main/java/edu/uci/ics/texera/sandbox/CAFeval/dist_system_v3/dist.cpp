#include "caf/all.hpp"
#include "caf/io/all.hpp"
#include "caf/string_algorithms.hpp"
#include "./Manager.hpp"


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
			add_message_type<vector<vector<string>>>("v<v<s>>");
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
			anon_send(*manager, connect_atom::value, string(hostname), *res);
			
		cout << "*** running on port: "
			<< *res << endl
			<< "*** press <enter> to shutdown" << endl;
		getchar();
		anon_send(*manager, disconnect_atom::value, string(hostname), cfg.port);
		system.await_actors_before_shutdown();
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
		cout << "command promopt: json_filename  num_workers  batch_size" << endl;
		while (1)
		{
			string input;
			getline(cin,input);
			if (input == "quit")
			{
				anon_send_exit(manager, exit_reason::kill);
				return;
			}
			auto cmd = split(input, ' ');
			if (cmd.size() == 3)
			{
				try { anon_send(manager, work_atom::value, cmd[0], stoi(cmd[1]), stoi(cmd[2])); }
				catch (...) { cout << "invaild command!" << endl; }
			}
			else if (cmd.size() == 2)
			{
				if (cmd[0] == "pause")
				{
					try { anon_send(manager, pause_atom::value, stoi(cmd[1])); }
					catch (...) { cout << "invaild command!" << endl; }
				}
				else if (cmd[0] == "resume")
				{
					try { anon_send(manager, resume_atom::value, stoi(cmd[1])); }
					catch (...) { cout << "invaild command!" << endl; }
				}
				else
					cout << "invaild command!" << endl;
			}
			else
				cout << "invaild command!" << endl;
		}
		system.await_actors_before_shutdown();
	}


	void caf_main(actor_system& system, const config& cfg)
	{
		gethostname(hostname, HOST_NAME_MAX);
		auto f = cfg.is_manager ? run_manager : run_worker;
		f(system, cfg);
	}

}

CAF_MAIN(io::middleman)
