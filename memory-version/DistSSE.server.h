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

#define min(x ,y) ( x < y ? x : y)

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerWriter;
using grpc::Status;

namespace DistSSE{

class DistSSEServiceImpl final : public RPC::Service {
private:	
	// static rocksdb::DB* ss_db;
	// rocksdb::DB* cache_db;
	static std::map<std::string, std::string> encrypted_db;
	std::map<std::string, std::string> cache_db;

	static std::mutex edb_mtx;
	std::mutex cdb_mtx;

    int MAX_THREADS;

public:
	DistSSEServiceImpl(int concurrent){
		
		MAX_THREADS = concurrent; //std::thread::hardware_concurrency();
	}

	static int store(const std::string l, const std::string e){
		{
			std::lock_guard<std::mutex> lock(edb_mtx);		
			encrypted_db[l] = e;
		}
	}

	static std::string get(const std::string l){

		std::string e = "";
		std::map<std::string, std::string>::iterator it;		
		
		it = encrypted_db.find(l);
		
		if (it != encrypted_db.end()) {
			e = it->second;
		}
		return e;
	}


	static void parse (std::string str, std::string& op, std::string& ind) {
		op = str.substr(0, 1);		
		ind = str.substr(1, 7); // TODO
	}

	static void search_task(int threadID, std::string kw, int begin, int end, std::set<std::string>* result_set) {
		std::string ind, op;
		std::string l, e, value;

		for(int i = begin + 1; i <= end; i++) {
			l = Util::H1(kw + std::to_string(i));
			// logger::log(logger::INFO) << "server.search(), l:" << l << ", kw: " << kw <<std::endl;
			e = get(l);
			value = Util::Xor( e, Util::H2(kw + std::to_string(i)) );

			parse(value, op, ind);

			// logger::log(logger::INFO) << "value: " << value <<std::endl;
			/*if(op == "1")*/  result_set->insert(value); // TODO
			// else result_set->erase(ind);
			if (i % 1000 == 0) logger::log(logger::INFO) << "Thread ID: " << threadID << ", searched: " << i << "\n" <<std::flush;
		}
	}

	static void merge( std::set<std::string>* searchResult, int counter, std::set<std::string> &mergeResult){
		for (int i = 0; i < counter; i++) {
			for (auto& t : searchResult[i]) {
				mergeResult.insert(t);
			}
		}
	}

	void search(std::string kw, std::string tw, int uc, std::set<std::string>& ID){
	
		std::vector<std::string> op_ind;

		std::string ind, op;
		std::string l, e, value;
		std::string cache_ind;

		int counter = 0;

		struct timeval t1, t2;		

		gettimeofday(&t1, NULL);

		cache_ind = cache_db[tw];
		Util::split(cache_ind, '|', ID); // get all cached inds

		if(kw == "") {
		gettimeofday(&t2, NULL);

		logger::log(logger::INFO) <<"[ <== ONLY CACHE ==> ] "<<"ID.size():"<< ID.size() <<" ,search time: "<< ((t2.tv_sec - t1.tv_sec) * 1000000.0 + t2.tv_usec -t1.tv_usec) /1000.0/ID.size()<<" ms" <<std::endl;

			return; // if kw == "", no need to search on ss_db.
		}

		//================ God bless =================
		std::set<std::string>* result_set = new std::set<std::string>[MAX_THREADS]; // result ID lists for storage nodes

		std::vector<std::thread> threads;
		int step = uc / MAX_THREADS;

        for (int i = 0; i < MAX_THREADS; i++) {
        	threads.push_back( std::thread(search_task, i,  kw, i * step, min((i + 1) * step, uc), &(result_set[i])) );
		}
		// join theads
    	for (auto& t : threads) {
        	t.join();
    	}

		merge(result_set, MAX_THREADS, ID);	
		
/*
		for(int i = 1; i <= uc; i++) {
			l = Util::H1(kw + std::to_string(i));
			// logger::log(logger::INFO) << "server.search(), l:" << l << ", kw: " << kw <<std::endl;
			e = get(ss_db, l);
			value = Util::Xor( e, Util::H2(kw + std::to_string(i)) );
			// std::vector<std::string> op_ind;
			ID.insert(value);
			Util::split(value, '|', op_ind);
			op = op_ind[0];
			ind = op_ind[1];
			if(op == "1") result_set->insert(ind); // TODO
			else result_set->erase(ind);
		}
*/

		gettimeofday(&t2, NULL);

  		logger::log(logger::INFO) <<"ID.size():"<< ID.size() <<" ,search time: "<< ((t2.tv_sec - t1.tv_sec) * 1000000.0 + t2.tv_usec - t1.tv_usec) /1000.0/ID.size()<<" ms" <<std::endl;

		//==============================================

		std::string ID_string = "";
		for (std::set<std::string>::iterator it=ID.begin(); it!=ID.end(); ++it){
    		ID_string += Util::str2hex(*it) + "|";
		}
		cache_db[tw] = ID_string;

	}

// server RPC
	// search() 实现搜索操作
	Status search(ServerContext* context, const SearchRequestMessage* request,
                  ServerWriter<SearchReply>* writer)  {

		std::string kw = request->kw();
		std::string tw = request->tw();	
		int uc = request->uc();
		
		struct timeval t1, t2;

		// TODO 读取数据库之前要加锁，读取之后要解锁
		
		std::set<std::string> ID;

		logger::log(logger::INFO) << "searching... " <<std::endl;

		// gettimeofday(&t1, NULL);
		search(kw, tw, uc, ID);
		// gettimeofday(&t2, NULL);

  		// logger::log(logger::INFO) <<"ID.size():"<< ID.size() <<" ,search time: "<< ((t2.tv_sec - t1.tv_sec) * 1000000.0 + t2.tv_usec - t1.tv_usec) /1000.0/ID.size()<<" ms" <<std::endl;
		// TODO 读取之后需要解锁

		SearchReply reply;
		
		for(int i = 0; i < ID.size(); i++){
			reply.set_ind(std::to_string(i));
			writer->Write(reply);
		}

		logger::log(logger::INFO) << "search done." <<std::endl;

	    return Status::OK;
  	}
	
	// update()实现单次更新操作
	Status update(ServerContext* context, const UpdateRequestMessage* request, ExecuteStatus* response) {
		std::string l = request->l();
		std::string e = request->e();
		//std::cout<<"ut: "<<ut<< " # " <<"enc_value: "<<enc_value<<std::endl;
		// TODO 更新数据库之前要加锁
		//logger::log(logger::INFO) <<".";
		int status = store(l, e);
		// TODO 更新之后需要解锁
		//logger::log(logger::INFO) << "*" << std::endl;
		logger::log(logger::INFO) << "UPDATE fuck" << std::endl;
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
		// TODO 读取数据库之前要加锁，读取之后要解锁
		UpdateRequestMessage request;
		while (reader->Read(&request)){
			l = request.l();
			e = request.e();
			store(l, e);
		}
		// TODO 读取之后需要解锁

		response->set_status(true);
		return Status::OK;
	}
};

}// namespace DistSSE

// static member must declare out of main function !!!
std::map<std::string, std::string> DistSSE::DistSSEServiceImpl::encrypted_db;
std::mutex DistSSE::DistSSEServiceImpl::edb_mtx;


void RunServer(int concurrent) {
  std::string server_address("0.0.0.0:50051");
  DistSSE::DistSSEServiceImpl service(concurrent);

  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());

  builder.RegisterService(&service);

  std::unique_ptr<Server> server(builder.BuildAndStart());
  DistSSE::logger::log(DistSSE::logger::INFO) << "Server listening on " << server_address << std::endl;

  server->Wait();
}

#endif // DISTSSE_SERVER_H
