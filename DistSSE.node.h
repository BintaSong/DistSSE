/*
 * Created by Xiangfu Song on 10/21/2016.
 * Email: bintasong@gmail.com
 * 
 * Server is storage nodes, communicate with proxy.
 **/
#ifndef DISTSSE_NODE_H
#define DISTSSE_NODE_H

#include <grpc++/grpc++.h>

#include "DistSSE.grpc.pb.h"

#include "DistSSE.Util.h"

#include "logger.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerWriter;
using grpc::Status;

namespace DistSSE{

class DistSSEServerServiceImpl final : public nodeRPC::Service {
private:	
	rocksdb::DB* ss_db;

public:
	DistSSEServerServiceImpl(const std::string db_path){
		rocksdb::Options options;
    	options.create_if_missing = true;
    	rocksdb::Status status = rocksdb::DB::Open(options, db_path, &ss_db);
	}

	int store(const std::string ut, const std::string e){
		rocksdb::Status s = ss_db->Put(rocksdb::WriteOptions(), ut, e);
		if (s.ok())	return 0;
		else return -1;
	}

	std::string get(const std::string ut){
		std::string tmp;
		rocksdb::Status s = ss_db->Get(rocksdb::ReadOptions(), ut, &tmp);
		if (s.ok())	return tmp;
		else return "";
	}

	int fetch_enc_file(void* filename){
		// TODO
	
	}
	
	// return the number of operations, including repeated operation
	int search(std::string enc_token, std::string _st, std::set<std::string>& ID){
	
		std::vector<std::string> ind_op_st;

		std::string ind, op;
		std::string ut, value, enc_value;
		std::string st = _st;
		int counter = 0;
		while(st != "NULL"){
			ut = Util::H1(enc_token + st); // 0.001 ms
			enc_value = get(ut); // 0.0004 ms
			// std::cout<< "IN SEARCH: "<< enc_value <<std::endl;
			/*
			if(enc_value == ""){
				std::cout<<"in search, error!"<<std::endl;
			}
			*/
			value = Util::Dec(st.c_str(), st.size(), enc_value);// 0.003 ms
			ind_op_st = Util::split(value, '|');// 0.001 ms
			ind = ind_op_st[0];
			op = ind_op_st[1];
		    st = Util::hex2str(ind_op_st[2]); // 0.001 ms
			if (op == "ADD") ID.insert(ind);
			else ID.erase(ind);
			counter++;
			if (counter % 10000 == 0) logger::log(logger::INFO) << " searching :  "<< counter << "\r"<< std::flush;
		}
		return counter;
	}

// server RPC
	// search() 实现搜索操作
	Status search(ServerContext* context, const SearchRequestMessage* request,
                  ServerWriter<SearchReply>* writer)  {

		std::string enc_token = request->enc_token();
		std::string st = request->st();
		
		struct timeval t1, t2;

		// TODO 读取数据库之前要加锁，读取之后要解锁
		
		std::set<std::string> ID;

		std::cout<< "node search token: " << enc_token <<std::endl;
		
		gettimeofday(&t1, NULL);
		
		// perform search
		int counter = search(enc_token, st, ID);

		gettimeofday(&t2, NULL);
  		logger::log(logger::INFO) <<"search time: "<< "operation counter: "<< counter <<", result size: "<<ID.size()<<", time: "<< ((t2.tv_sec - t1.tv_sec) * 1000000.0 + t2.tv_usec -t1.tv_usec) /1000.0/counter <<" ms" <<std::endl;
		// TODO 读取之后需要解锁

		SearchReply reply;
		
		for(int i = 0; i < ID.size(); i++){
			reply.set_ind("fuck");
			writer->Write(reply);
		}

	    return Status::OK;
  	}
	
	// update()实现单次更新操作
	Status update(ServerContext* context, const UpdateRequestMessage* request, ExecuteStatus* response) {
		std::string ut = request->ut();
		std::string enc_value = request->enc_value();
		//std::cout<<"ut: "<<ut<< " # " <<"enc_value: "<<enc_value<<std::endl;
		// TODO 更新数据库之前要加锁
		//logger::log(logger::INFO) <<".";
		store(ut, enc_value);
		// TODO 更新之后需要解锁
		//logger::log(logger::INFO) << "*" << std::endl;
		// logger::log(logger::INFO) << "UPDATE COMPLETE" << std::endl;
		response->set_status(true);
		return Status::OK;
	}
	
	// batch_update()实现批量更新操作
	Status batch_update(ServerContext* context, ServerReader< UpdateRequestMessage >* reader, ExecuteStatus* response) {
		std::string ut;
		std::string enc_value;
		// TODO 读取数据库之前要加锁，读取之后要解锁
		UpdateRequestMessage request;
		while (reader->Read(&request)){
			ut = request.ut();
			enc_value = request.enc_value();
			store(ut, enc_value);
		}
		// TODO 读取之后需要解锁

		response->set_status(true);
		return Status::OK;
	}
};

}// namespace DistSSE

void RunServer(std::string db_path) {
  std::string server_address("0.0.0.0:50052");
  DistSSE::DistSSEServerServiceImpl service(db_path);

  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());

  builder.RegisterService(&service);

  std::unique_ptr<Server> server(builder.BuildAndStart());
  DistSSE::logger::log(DistSSE::logger::INFO) << "Storage node listening on " << server_address << std::endl;

  server->Wait();
}

#endif // DISTSSE_NODE_H
