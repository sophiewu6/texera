#include "caf/all.hpp"
#include "caf/io/all.hpp"
#include "./base.hpp"

using namespace std;
using namespace caf;
using namespace rapidjson;

namespace Texera
{

	using Scan_op = typed_actor<reacts_to<on_work>>;
	class Scan : public Scan_op::base
	{
	public:
		static constexpr const char* class_str = "Scan";
		static constexpr bool has_weak_ptr_semantics = false;
		Scan(actor_config& cfg,string hostname="", int bs=0,string filename=""):Scan_op::base(cfg){
			next = *(system().middleman().remote_actor(hostname, 10086));
			system().middleman().publish(this, 10086);
			batch_size = bs;
			file = ifstream(filename);
			send(this, on_work::value);
		}
		behavior_type make_behavior() override {
			return
			{
				[=](on_work)
				{
					vector<vector<string>> res;
					for (int i = 0; i < batch_size; ++i)
					{
						if (!file.good())
							break;
						string str;
						getline(file, str);
						res.emplace_back(split(str, ','));
					}
					anon_send(next, on_work::value, move(res));
					send(this, on_work::value);
					if (!file.good())
					{
						anon_send(next, on_finish::value);
						quit();
					}
				}
			};
		}
	private:
		actor next;
		int batch_size;
		ifstream file;
	};


	using Keyword_op = typed_actor<reacts_to<on_work, vector<vector<string>>>, reacts_to<on_finish>>;
	class Keyword :public Keyword_op::base
	{
	public:
		static constexpr const char* class_str = "Keyword";
		static constexpr bool has_weak_ptr_semantics = false;
		Keyword(actor_config& cfg, string hostname="",int i=0,string key="") : Keyword_op::base(cfg) {
			next=*(system().middleman().remote_actor(hostname, 10086));
			system().middleman().publish(this, 10086);
			idx = i;
			keyword = key;
		}
		behavior_type make_behavior() override {
			return
			{
				[=](on_work,vector<vector<string>>& d)
				{
					for (auto i = d.begin(); i != d.end();)
					{
						auto& to_search = *i;
						if (to_search.size() <= idx || to_search[idx].find(keyword) == string::npos)
							i = d.erase(i);
						else
							++i;
					}
					d.shrink_to_fit();
					anon_send(next, on_work::value, move(d));
				},
				[=](on_finish)
				{
					anon_send(next,on_finish::value);
					quit();
				}
			};
		}
	private:
		actor next;
		string keyword;
		int idx;
	};

	using Filter_op = typed_actor<reacts_to<on_work, vector<vector<string>>>, reacts_to<on_finish>>;
	class Filter :public Filter_op::base
	{
	public:
		static constexpr const char* class_str = "Filter";
		static constexpr bool has_weak_ptr_semantics = false;
		Filter(actor_config& cfg, string hostname = "", vector<int> is = {}) : Filter_op::base(cfg) {
			next = *(system().middleman().remote_actor(hostname, 10086));
			system().middleman().publish(this, 10086);
			idxs = is;
		}
		behavior_type make_behavior() override {
			return
			{
				[=](on_work, vector<vector<string>>& d)
				{
					for (auto i = d.begin(); i != d.end(); ++i)
					{
						vector<string> temp;
						for (auto& j : idxs)
							temp.emplace_back((*i)[j]);
						(*i).swap(temp);
					}
					anon_send(next, on_work::value, move(d));
				},
				[=](on_finish)
				{
					anon_send(next,on_finish::value);
					quit();
				}
			};
		}
	private:
		actor next;
		vector<int> idxs;
	};

	using Sink_op = typed_actor<reacts_to<on_work, vector<vector<string>>>, reacts_to<on_finish>>;
	class Sink :public Sink_op::base
	{
	public:
		static constexpr const char* class_str = "Sink";
		static constexpr bool has_weak_ptr_semantics = false;
		Sink(actor_config& cfg, actor_addr addr=nullptr) : Sink_op::base(cfg) {
			next = actor_cast<actor>(addr);
			system().middleman().publish(this, 10086);
		}
		behavior_type make_behavior() override {
			return
			{
				[=](on_work,vector<vector<string>>& work)
				{
				/*
					for (auto i : work)
					{
						cout << " sink:";
						for (auto j : i)
							cout << j << " ";
						cout << endl;
					}
					*/
				},
				[=](on_finish)
				{
					anon_send(next,on_finish::value);
				}
			};
		}
	private:
		actor next;
	};




	class MotherGoose : public event_based_actor
	{
	public:
		MotherGoose(actor_config& cfg) :event_based_actor(cfg) {

		}
		behavior make_behavior() override {
			return
			{
				[=](string type,string hostname , int bs ,string filename)
			{
				if (type == "Scan")
					system().spawn<Scan>(hostname,bs,filename);
				else if (type == "Keyword")
					system().spawn<Keyword>(hostname, bs, filename);
			},
				[=](string type,string hostname,vector<int>& is)
			{
				if (type == "Filter")
					system().spawn<Filter>(hostname, is);
			},
				[=](string type,actor_addr addr)
			{
				if (type == "Sink")
					system().spawn<Sink>(addr);
			}
			};
		}
	};

}