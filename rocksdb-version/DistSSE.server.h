/*
 * Created by Xiangfu Song on 10/21/2016.
 * Email: bintasong@gmail.com
 * 
 */
#ifndef DISTSSE_SERVER_H
#define DISTSSE_SERVER_H

#include <grpc++/grpc++.h>

#include "DistSSE.grpc.pb.h"

#include "DistSSE.Util.h"

#include "logger.h"

#include "thread_pool.h"
#include <unordered_set>


#define min(x ,y) ( (x) < (y) ? (x) : (y) )
#define max(x, y) ( (x) < (y) ? (y) : (x) )

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerWriter;
using grpc::Status;


namespace DistSSE{

class DistSSEServiceImpl final : public RPC::Service {
private:	
	static rocksdb::DB* ss_db;
	// static rocksdb::DB* ss_db_read;
	static rocksdb::DB* cache_db;
    int MAX_THREADS;

	static std::mutex result_mtx;
	//static std::mutex cache_write_mtx;

public:
	DistSSEServiceImpl(const std::string db_path, std::string cache_path, int concurrent){

  		signal(SIGINT, abort);

		rocksdb::Options options;
    	options.create_if_missing = true;
	    Util::set_db_common_options(options);
		// options.use_fsync = true;

		rocksdb::Status s1 = rocksdb::DB::Open(options, db_path, &ss_db);
		// rocksdb::Status s2 = rocksdb::DB::OpenForReadOnly(options, db_path, &ss_db_read);

		// set options for merge operation
		rocksdb::Options simple_options;
		simple_options.create_if_missing = true;
		simple_options.merge_operator.reset(new rocksdb::StringAppendOperator() );
		simple_options.use_fsync = true;

		rocksdb::Status s3 = rocksdb::DB::Open(simple_options, cache_path, &cache_db);

		if(!s1.ok()) {
			std::cerr<< "open ssdb error:"<<s1.ToString() <<std::endl;	
		}
		// assert(s2.ok());
		assert(s3.ok());

		MAX_THREADS = concurrent; //std::thread::hardware_concurrency();
	}

	static void abort( int signum )
	{
		delete ss_db;
		delete cache_db; 
		logger::log(logger::INFO)<< "abort: "<< signum <<std::endl;
	   	exit(signum);
	}

	static int store(rocksdb::DB* &db, const std::string l, const std::string e){
		rocksdb::Status s; 	
		rocksdb::WriteOptions write_option = rocksdb::WriteOptions();
		// write_option.sync = true;
		// write_option.disableWAL = false;
		{
			//std::lock_guard<std::mutex> lock(ssdb_write_mtx);		
			s = db->Put(write_option, l, e);
		}

		assert(s.ok());
		if (s.ok())	return 0;
		else return -1;
	}

	static std::string get(rocksdb::DB* &db, const std::string l){
		std::string tmp;
		rocksdb::Status s;
	
	//redo:
		{
			//std::lock_guard<std::mutex> lock(ssdb_write_mtx);
			s = db->Get(rocksdb::ReadOptions(), l, &tmp);
		}

		//if (!s.ok())	std::cerr << "in get() " << s.ToString()<<", tmp: "<< tmp << std::endl;
		
 	 return tmp;
	}

	static bool get(rocksdb::DB* &db, const std::string l, std::string& e){
		rocksdb::Status s;
	
		{
			//std::lock_guard<std::mutex> lock(ssdb_write_mtx);
			s = db->Get(rocksdb::ReadOptions(), l, &e);
		}
		
		return s.ok(); 
	}

	static int merge(rocksdb::DB* &db, const std::string l, const std::string append_str) {

		int status = -1;
		
		try{

			rocksdb::WriteOptions write_option = rocksdb::WriteOptions();
			write_option.sync = true;

			rocksdb::Status s;
			{
				//std::lock_guard<std::mutex> lock(cache_write_mtx);
				s = db->Delete(write_option, l);
				s = db->Put(write_option, l, append_str);
			}

			if (s.ok())	status = 0;

		}catch(std::exception &e) {
			std::cerr << "in append_cache() " << e.what()<< std::endl;
			exit(1);
		}

		return status;
	}

	static int delete_entry(rocksdb::DB* &db, const std::string l) {
		int status = -1;
		
		try{

			rocksdb::WriteOptions write_option = rocksdb::WriteOptions();
			//write_option.sync = true;

			rocksdb::Status s;
				
			s = db->Delete(write_option, l);

			if (s.ok())	status = 0;

		}catch(std::exception &e) {
			std::cerr << "in delete_entry() " << e.what()<< std::endl;
			exit(1);
		}

		return status;
	}

	static void search_log(std::string kw, double search_time, int result_size) {
		// std::ofstream out( "search.slog", std::ios::out|std::ios::app);
		byte k_s[17] = "0123456789abcdef";
		byte iv_s[17] = "0123456789abcdef";

		std::string keyword = Util::dec_token(k_s, AES128_KEY_LEN, iv_s, kw);
			
		std::string word = keyword == "" ? "cached" : keyword;	
		double per_entry_time = result_size == 0 ? search_time : (search_time/result_size);
		std::cout <<  word + "\t" + std::to_string(result_size)+ "\t" + std::to_string(search_time) + "\t" + std::to_string(per_entry_time) << std::endl;
	}

	static void parse (std::string str, std::string& op, std::string& ind) {
		op = str.substr(0, 1);		
		ind = str.substr(1, 7); // TODO
	}


	static void search_task(std::string kw, int begin, int max, int step, std::set<std::string>* result_set) {
		std::string ind, op;
		std::string l, e, value;

		bool flag = false;
		for(int i = begin ; i <= max; i += step) {

			l = Util::H1(kw + std::to_string(i));
			e = get(ss_db, l);
			if (e.compare("") == 0) {
				logger::log(logger::ERROR) << "ERROR in search, null str found: " << l << l.length() <<std::endl;
				continue;
			}
			
			value = Util::Xor( e, Util::H2(kw + std::to_string(i)) );

			parse(value, op, ind);
			
			{
				std::lock_guard<std::mutex> lock(result_mtx);
				if(op == "1") result_set->insert(value); // TODO
				// else result_set->erase(ind);
				// if (i % 1000 == 0) logger::log(logger::INFO) << "Thread ID: " << threadID << ", searched: " << i << "\n" <<std::flush;
 			}

			//  int s = delete_entry(ss_db, l);	
		}
	}


	static void search_single(std::string tw, std::string kw, int max, std::unordered_set<std::string>& cache_ID, std::unordered_set<std::string>& update_ID, std::string& cache_string, std::string& update_string) {
		std::string l, e, ind, op, value;
		// std::string cache_string, update_string;
		std::mutex res_mutex;

		auto read_cache_job = [&tw, &cache_ID, &cache_string] ( ) {
			cache_string = get(cache_db, tw);

			//res_mutex.lock();	
			Util::split(cache_string, '|', cache_ID);//TODO
			//res_mutex.unlock();
		};

		std::thread cache_thread(read_cache_job);

		for(int i = 1; i <= max; i++) {

			l = Util::H1(kw + std::to_string(i));

			e = get(ss_db, l);

			if ( e.compare("") == 0) {
				logger::log(logger::ERROR) << "we were supporsed to find something!"<<std::endl;
				continue;			
			}
			
			value = Util::Xor(e, Util::H2(kw + std::to_string(i)) );

			parse(value, op, ind);
			
			if(op == "1") {
				res_mutex.lock();	
				update_ID.insert(Util::str2hex(ind));
				res_mutex.unlock();	
			}

			update_string += Util::str2hex(ind) + "|";
		}

		cache_thread.join();
		//merge(cache_db, tw, cache_string + update_string);

	}

	static void merge( std::set<std::string>& searchResult,  std::string& merge_string){
	
			for (auto& t : searchResult) {
				merge_string += Util::str2hex(t) + "|";
			}
	}

	/*void search_parallel (std::string kw, std::string tw, int uc, std::set<std::string>& result){
		// std::set<std::string> result;
		std::string cache_string, result_string;

		ThreadPool fetch_pool(2);
		ThreadPool decrypt_pool(1);
		static struct timeval l, r;
	
		auto read_cache_job = [&tw, &result, &cache_string] () {
			cache_string = get(cache_db, tw);
		};

		auto decrypt_job = [&result_string, &result] (const std::string st_c_, const std::string e) {
			//gettimeofday(&l, NULL);
			std::string value = Util::Xor( e, Util::H2(st_c_) );
			std::string op, ind;			
			parse(value, op, ind);
			result.insert(ind);
			result_string += Util::str2hex(ind) + "|";
			//gettimeofday(&r, NULL);
			//time  +=  ((r.tv_sec - l.tv_sec) * 1000000.0 + r.tv_usec - l.tv_usec) / 1000.0;
		};
	
		auto lookup_job = [&decrypt_job, &decrypt_pool]( const std::string st_c_, const std::string u){
			std::string e;
			gettimeofday(&l, NULL);
			bool found = get(ss_db, u, e);
			gettimeofday(&r, NULL);
			// time  +=  ((r.tv_sec - l.tv_sec) * 1000000.0 + r.tv_usec - l.tv_usec) / 1000.0;
			if(found) {
				decrypt_pool.enqueue(decrypt_job, st_c_, e);
			}else{
				logger::log(logger::ERROR) << "We were supposed to find something!" << std::endl;
			}
		};


		auto derive_job = [&kw, &tw, &lookup_job, &fetch_pool]( const int begin, const int max, const int step) {
			for(int i = begin; i <= max; i += step) {
				std::string st_c_ = kw + std::to_string(i);
				std::string u = Util::H1(st_c_);
				fetch_pool.enqueue(lookup_job, st_c_, u);
			}
		};
		
		std::vector<std::thread> threads;
    
		unsigned n_threads = MAX_THREADS - 3;
		

		threads.push_back( std::thread(read_cache_job) );

		for( int i = 1; i <= n_threads; i++) { // counter begin from 1 !
			threads.push_back( std::thread(derive_job, i, uc, n_threads) );	
		}

		for( int i = 0; i <= n_threads; i++) { // counter begin from 1 !
			threads[i].join();
		}
		
		fetch_pool.join();
		decrypt_pool.join();

 		merge(cache_db, tw, cache_string + result_string);
		// return result;
	}*/

	 void search_parallel_simple(std::string kw, std::string tw, int uc, int fetch_threads, std::unordered_set<std::string>& cache_ID, std::unordered_set<std::string>& result, std::string& cache_string, std::string& result_string) {
		std::mutex res_mutex;
	
		auto fetch_job = [&kw, &tw, &res_mutex, &result, &result_string] ( int begin, int max, int step) {
			 for(int i = begin; i <= max; i += step) {

                                std::string st_c_ = kw + std::to_string(i);
                                std::string u = Util::H1(st_c_);
                                std::string e;

                                bool found = get(ss_db, u, e);

                                if(found) {
					std::string value = Util::Xor( e, Util::H2(st_c_) );
                        		std::string op, ind;
                        		parse(value, op, ind);

                       			if (step > 1) res_mutex.lock();
                        			result.insert(Util::str2hex(ind));
                        			result_string += Util::str2hex(ind) + "|";
                       		 	if (step > 1) res_mutex.unlock();
                                }else{
                                        logger::log(logger::ERROR) << "We were supposed to find something!" << std::endl;
                                }
                        }

		};

		get(cache_db, tw, cache_string);
	//	Util::split(cache_string, '|', cache_ID);	
		std::vector<std::thread> threads;

                for( int i = 1; i <= fetch_threads; i++) { // counter begin from 1 !
                        threads.push_back( std::thread(fetch_job, i, uc, fetch_threads) );
                }

                for (auto& t : threads) {
                        t.join();
                }

	}

	void search_parallel(std::string kw, std::string tw, int uc, int decrypt_threads, std::unordered_set<std::string>& cache_ID, std::unordered_set<std::string>& result, std::string& cache_string, std::string& result_string){
		// std::set<std::string> result;
		//std::string cache_string, result_string;

		std::mutex res_mutex;

		ThreadPool decrypt_pool(decrypt_threads);

		static double time = 0.0;
	
		auto read_cache_job = [&tw, &cache_ID, &res_mutex, &cache_string] ( ) {
			cache_string = get(cache_db, tw);

			//res_mutex.lock();		
			Util::split(cache_string, '|', cache_ID); //TODO
			//res_mutex.unlock();		
		};

		auto decrypt_job = [&result_string, &result, &res_mutex, &decrypt_threads] (const std::string st_c_, const std::string e) {

			std::string value = Util::Xor( e, Util::H2(st_c_) );
			std::string op, ind;		
			parse(value, op, ind);

			if (decrypt_threads > 1) res_mutex.lock();
			result.insert(Util::str2hex(ind));
			result_string += Util::str2hex(ind) + "|";
			if (decrypt_threads > 1) res_mutex.unlock();
		};
	
		auto fetch_job = [&kw, &tw, &decrypt_job, &decrypt_pool]( const int begin, const int max, const int step) {

			for(int i = begin; i <= max; i += step) {

				std::string st_c_ = kw + std::to_string(i);
				std::string u = Util::H1(st_c_);
				std::string e;

				bool found = get(ss_db, u, e);

				if(found) {
					decrypt_pool.enqueue(decrypt_job, st_c_, e);
				}else{
					logger::log(logger::ERROR) << "We were supposed to find something!" << std::endl;
				}
			}

		};
		
		std::vector<std::thread> threads;
    
		unsigned n_threads = MAX_THREADS - decrypt_threads;

		threads.push_back( std::thread(read_cache_job) );

		for( int i = 1; i <= n_threads; i++) { // counter begin from 1 !
			threads.push_back( std::thread(fetch_job, i, uc, n_threads) );	
		}

		for (auto& t : threads) {
			t.join();
		}
		
		decrypt_pool.join();

 		//merge(cache_db, tw, cache_string + result_string);
		// return result;
	}


	void search(std::string kw, std::string tw, int uc, std::set<std::string>& ID){
	
		std::vector<std::string> op_ind;

		std::string ind, op;
		std::string l, e, value;
		std::string cache_ind;

		int counter = 0;

		struct timeval t1, t2;

		double search_time;

		try{
			
			gettimeofday(&t1, NULL);

			std::string merge_string = get(cache_db, tw);
			// Util::split(merge_string, '|', ID); // get all cached inds					
			gettimeofday(&t2, NULL);

			//std::string merge_string = cache_ind;
			
			if(kw != "") {

				/*if (uc < 250) {

					search_single( kw, 1, uc, ID, merge_string );
	
				}else */{
										
					//std::set<std::string> result_set;//[MAX_THREADS]; // result ID lists for storage nodes

					std::vector<std::thread> threads;
				
					for (int i = 1; i <= MAX_THREADS; i++) {
						threads.push_back( std::thread(search_task, kw, i, uc, MAX_THREADS, &ID));
					}

					// join theads
					for (auto& t : threads) {
						t.join();
					}
					gettimeofday(&t2, NULL);
					merge(ID, merge_string);
				}
				// merge to cache can be done by a seperate thread backgroud, the result can be returned before.
				// so we don't count the time...
			}
			
			//gettimeofday(&t2, NULL);		

			if (merge_string != "") {
				int s = merge(cache_db, tw, merge_string);
				assert(s == 0);
			}

			search_time =  ((t2.tv_sec - t1.tv_sec) * 1000000.0 + t2.tv_usec - t1.tv_usec) / 1000.0 ;

			search_log(kw, search_time, ID.size());


		}catch(const std::exception& e)
		{
			std::cerr << "in Search() " << e.what()<< std::endl;
			exit(1);
		}
	}


// server RPC
	// search() 实现搜索操作
	Status search(ServerContext* context, const SearchRequestMessage* request,
                  ServerWriter<SearchReply>* writer) {

		std::string kw = request->kw();
		std::string tw = request->tw();	
		int uc = request->uc();
		
		struct timeval t1, t2;

		std::unordered_set<std::string> cache_ID, update_ID;

		std::string cache_string, update_string;
		gettimeofday(&t1, NULL);

		//if (uc <= 100) search_single(tw, kw, uc, cache_ID, update_ID, cache_string, update_string);
		//else search_parallel(kw, tw, uc, 2, cache_ID, update_ID, cache_string, update_string);
		search_parallel_simple(kw, tw, uc, 4, cache_ID, update_ID, cache_string, update_string);

		gettimeofday(&t2, NULL);
		
		double search_time =  ((t2.tv_sec - t1.tv_sec) * 1000000.0 + t2.tv_usec - t1.tv_usec) / 1000.0 ;

		search_log( kw, search_time, cache_ID.size() + update_ID.size() );
  		
		merge(cache_db, tw, cache_string + update_string);
	
		//SearchReply reply;
		//write->Write(cache_string + update_string);
		//for(int i = 0; i < update_ID.size(); i++){
		//	reply.set_ind(std::to_string(i));
		//	writer->Write(reply);
		//}

	    return Status::OK;
  	}
	
	// update()实现单次更新操作
	Status update(ServerContext* context, const UpdateRequestMessage* request, ExecuteStatus* response) {
		std::string l = request->l();
		std::string e = request->e();
		//std::cout<<"in update(), counter:  "<< request->counter() <<std::endl;

		int status = store(ss_db, l, e);

		assert(status == 0);

		if(status != 0) {
			response->set_status(false);
			return Status::CANCELLED;
		}
		response->set_status(true);
		return Status::OK;
	}
	
	// batch_update()实现批量更新操作
	Status batch_update(ServerContext* context, ServerReader< UpdateRequestMessage >* reader, ExecuteStatus* response) {
		std::string l;
		std::string e;

		UpdateRequestMessage request;
		while (reader->Read(&request)) {
			l = request.l();
			e = request.e();
			//std::cout<<"in update(), counter:  "<<request.counter()<<std::endl;
			store(ss_db, l, e);
		  //  assert(status == 0);
		}

		response->set_status(true);
		return Status::OK;
	}

	// batch_cache(), only used for expriment simulation
	Status batch_cache(ServerContext* context, ServerReader< CacheRequestMessage >* reader, ExecuteStatus* response) {
		std::string tw;
		std::string inds;

		CacheRequestMessage request;
		while (reader->Read(&request)){
			tw = request.tw();
			inds = request.inds();
			int s = merge(cache_db, tw, inds);
			assert(s == 0);
		}

		response->set_status(true);
		return Status::OK;
	}
};

}// namespace DistSSE

// static member must declare out of main function !!!
rocksdb::DB* DistSSE::DistSSEServiceImpl::ss_db;
// rocksdb::DB* DistSSE::DistSSEServiceImpl::ss_db_read;
rocksdb::DB* DistSSE::DistSSEServiceImpl::cache_db;

std::mutex DistSSE::DistSSEServiceImpl::result_mtx;
//std::mutex DistSSE::DistSSEServiceImpl::cache_write_mtx;

void RunServer(std::string db_path, std::string cache_path, int concurrent) {


  std::string server_address("0.0.0.0:50051");
  DistSSE::DistSSEServiceImpl service(db_path, cache_path, concurrent);
  
  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());

  builder.RegisterService(&service);

  std::unique_ptr<Server> server(builder.BuildAndStart());
  DistSSE::logger::log(DistSSE::logger::INFO) << "Server listening on " << server_address << std::endl;

  server->Wait();
}

#endif // DISTSSE_SERVER_H
