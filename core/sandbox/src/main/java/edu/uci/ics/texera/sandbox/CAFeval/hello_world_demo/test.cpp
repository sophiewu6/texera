#include <set>
#include <map>
#include <vector>
#include <cstdlib>
#include <sstream>
#include <iostream>

#include "caf/all.hpp"
#include "caf/io/all.hpp"

#include "caf/string_algorithms.hpp"

using namespace std;
using namespace caf;

namespace {

	class config : public actor_system_config {
	public:
		std::string name,another_name;

		config() {
			opt_group{ custom_options_, "global" };
		}
	};



	behavior testee(event_based_actor* self)
	{
		return { [=]() {} };
	}


	void caf_main(actor_system& system, const config& cfg) {
		//call main function here
		//auto name = cfg.name.empty()?"Boss":cfg.name, another_name = cfg.another_name.empty()?"Worker":cfg.another_name;
		for (int i = 0; i < 1000000; ++i)
			system.spawn(testee);
	}

}


CAF_MAIN()