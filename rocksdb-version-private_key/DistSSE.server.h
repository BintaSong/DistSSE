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

using namespace CryptoPP;

byte iv_s[17] = "0123456789abcdef";

namespace DistSSE{

class DistSSEServiceImpl final : public RPC::Service {
private:	
	static rocksdb::DB* ss_db;
	rocksdb::DB* cache_db;
    int MAX_THREADS;

public:
	DistSSEServiceImpl(const std::string db_path, const std::string cache_path, int concurrent){
		rocksdb::Options options;

		    rocksdb::CuckooTableOptions cuckoo_options;
            cuckoo_options.identity_as_first_hash = false;
            cuckoo_options.hash_table_ratio = 0.9;
            
            
            cuckoo_options.use_module_hash = false;
            cuckoo_options.identity_as_first_hash = true;
            

            options.table_cache_numshardbits = 4;
            options.max_open_files = -1;
            
			options.allow_concurrent_memtable_write = true;
    		options.enable_write_thread_adaptive_yield = true;
            
			options.table_factory.reset(rocksdb::NewCuckooTableFactory(cuckoo_options));
            
            // options.memtable_factory.reset(new rocksdb::VectorRepFactory());
            
            options.compression = rocksdb::kNoCompression;
            options.bottommost_compression = rocksdb::kDisableCompressionOption;
            
            options.compaction_style = rocksdb::kCompactionStyleLevel;
            options.info_log_level = rocksdb::InfoLogLevel::INFO_LEVEL;
            
            
            // options.max_grandparent_overlap_factor = 10;
            
            options.delayed_write_rate = 8388608;
            options.max_background_compactions = 20;
            
            options.disableDataSync = true;
            options.allow_mmap_reads = true;
            options.new_table_reader_for_compaction_inputs = true;
            
            options.max_bytes_for_level_base = 4294967296;
            options.arena_block_size = 134217728;
            options.level0_file_num_compaction_trigger = 10;
            options.level0_slowdown_writes_trigger = 16;
            options.hard_pending_compaction_bytes_limit = 137438953472;
            options.target_file_size_base=201327616;
            options.write_buffer_size=1073741824;
    		options.create_if_missing = true;

    	rocksdb::Status s1 = rocksdb::DB::Open(options, db_path, &ss_db);
    	rocksdb::Status s2 = rocksdb::DB::Open(options, cache_path, &cache_db);
			
			if (!s1.ok() || !s2.ok()) {
                logger::log(logger::CRITICAL) << "Unable to open the database: " << s1.ToString() <<", s2:"<< s2.ToString()<< std::endl;
                // db_ = NULL;
            }
		MAX_THREADS = concurrent; //std::thread::hardware_concurrency();
	}

	static int store(rocksdb::DB* &db, const std::string l, const std::string e){
		rocksdb::Status s = db->Put(rocksdb::WriteOptions(), l, e);
		if (s.ok())	return 0;
		else return -1;
	}

	static std::string get(rocksdb::DB* &db, const std::string l){
		std::string tmp;
		rocksdb::Status s = db->Get(rocksdb::ReadOptions(), l, &tmp);
		if (s.ok())	return tmp;
		else return "";
	}


	static void parse (std::string str, std::string& op, std::string& ind, std::string& key) {
		op = str.substr(0, 1);		
		ind = str.substr(1, 8); // TODO
		key = str.substr(9, AES128_KEY_LEN);
	}
	
	void recover_st(std::string old_st, std::string key, std::string& new_st) {
		try {

			CFB_Mode< AES >::Decryption d;

			d.SetKeyWithIV((byte*) key.c_str(), AES128_KEY_LEN, iv_s, (size_t)AES::BLOCKSIZE); 
	
			byte tmp_new_st[old_st.length()];

			d.ProcessData(tmp_new_st, (byte*) old_st.c_str(), old_st.length());
			
			new_st = std::string((const char*)tmp_new_st, old_st.length());
		}
		catch(const CryptoPP::Exception& e)
		{
			std::cerr << "in generate_st() " << e.what()<< std::endl;
			exit(1);
		}
	}

	void search(std::string tw, std::string st, size_t uc, std::set<std::string>& ID){
	
		std::vector<std::string> op_ind;

		std::string op, ind, rand_key;
		std::string _st, l, e, value;
		// std::string cache_ind;

		int counter = 0;

		struct timeval t1, t2;

		gettimeofday(&t1, NULL);


		std::set<std::string> result_set;
	    _st = st;
		
		// logger::log(logger::INFO) << "uc: "<< uc <<std::endl;
		logger::log(logger::INFO) <<"In gen_search_token==>  " << "st:" << st << ", tw: " << tw << ", uc: "<< uc <<std::endl;
		for(size_t i = 1; i <= uc; i++) {
			l = Util::H1(tw + _st);

			e = get(ss_db, l);
			if(e == "") {
				logger::log(logger::ERROR) << "No found" <<std::endl;
				return;
			}
			value = Util::Xor( e, Util::H2(tw + _st) );
			// logger::log(logger::INFO) << "value: "<< value <<std::endl;
			ID.insert(value);

			 // parse(value, op, ind, rand_key);
            parse(value, op, ind, _st);
			/*if(op == "1")*/ result_set.insert(ind); // TODO
			//else result_set->erase(ind);
			
			// recover_st( _st, rand_key, _st );
		}

		gettimeofday(&t2, NULL);

  		logger::log(logger::INFO) <<"ID.size():"<< ID.size() <<" ,search time: "<< ((t2.tv_sec - t1.tv_sec) * 1000000.0 + t2.tv_usec - t1.tv_usec) /1000.0/ID.size()<<" ms" <<std::endl;


		std::string ID_string = "";
		for (std::set<std::string>::iterator it=ID.begin(); it!=ID.end(); ++it){
    		ID_string += Util::str2hex(*it) + "|";
		}
		store(cache_db, tw, ID_string);
	}

// server RPC
	// search() 实现搜索操作
	Status search(ServerContext* context, const SearchRequestMessage* request,
                  ServerWriter<SearchReply>* writer)  {

		std::string st = request->kw();
		std::string tw = request->tw();	
		size_t uc = request->uc();
		
		struct timeval t1, t2;

		// TODO 读取数据库之前要加锁，读取之后要解锁
		
		std::set<std::string> ID;

		logger::log(logger::INFO) << "searching... " <<std::endl;

		// gettimeofday(&t1, NULL);
		search(tw, st, uc, ID);
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
		int status = store(ss_db, l, e);
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
			store(ss_db, l, e);
		}
		// TODO 读取之后需要解锁

		response->set_status(true);
		return Status::OK;
	}
};

}// namespace DistSSE

// static member must declare out of main function !!!

rocksdb::DB* DistSSE::DistSSEServiceImpl::ss_db;

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
