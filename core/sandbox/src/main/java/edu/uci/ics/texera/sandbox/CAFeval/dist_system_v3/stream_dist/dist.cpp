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
			add_message_type<vector<int>>("v<i>");
			add_actor_type<Scan>("Scan");
			add_actor_type<Filter>("Filter");
			add_actor_type<Sink>("Sink");
			add_actor_type<Keyword>("Keyword");
			opt_group{ custom_options_, "global" }
				.add(is_manager, "server,s", "run in manager machines")
				.add(host, "host,H", "set host (ignored in manager machines)")
				.add(hostport, "hostport,r", "set host port")
				.add(port, "port,p", "set port (ignored in manager machines)");

		}
	};

	void run_worker(actor_system& system, const config& cfg)
	{
		auto mg = system.spawn<MotherGoose>();
		auto res = system.middleman().publish(mg, 4242);
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
		auto res = system.middleman().publish(manager, cfg.port);
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
			getline(cin, input);
			if (input == "quit")
			{
				anon_send_exit(manager, exit_reason::kill);
				return;
			}
			else
			{
				try { anon_send(manager, on_work::value, stoi(input)); }
				catch (...) { cout << "invaild command!" << endl; }
			}
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
