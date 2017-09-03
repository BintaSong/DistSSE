/* 
 * Created by Xiangfu Song on 10/21/2016.
 * Email: bintasong@gmail.com
 * 
 */
#ifndef DISTSSE_CLIENT_H
#define DISTSSE_CLIENT_H

#include <grpc++/grpc++.h>

#include "DistSSE.grpc.pb.h"

#include "DistSSE.Util.h"

#include "logger.h"

#include <ssdmap/bucket_map.hpp>

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReaderInterface;
using grpc::ClientWriterInterface;

using grpc::Status;

using namespace CryptoPP;

// 用来生成 kw
byte k_s[17] = "0123456789abcdef";
byte iv_s[17] = "0123456789abcdef";

/*
// 用来生成加密 label
byte k_l[17] = "abcdef1234567890";
byte iv_l[17] = "0123456789abcdef";

// 用来生成搜索 token
byte k_st[17] = "123456789abcdef0";
byte iv_st[17] = "0abcdef123456789";
*/

extern int max_keyword_length;
extern int max_nodes_number;

namespace DistSSE{

class Client {
private:
 	std::unique_ptr<RPC::Stub> stub_;
	rocksdb::DB* cs_db;
	rocksdb::DB* trace_db;
	
	// std::mutex st_mtx;
	// std::mutex uc_mtx;

	// std::map<std::string, std::string> st_mapper;	
	// std::map<std::string, size_t> uc_mapper;

	SHA256 hasher;

	ssdmap::bucket_map< std::string, std::string> client_map;

	std::mutex client_map_mtx;
	std::mutex trace_map_mtx;
	std::mutex client_hash_mtx;

public:

  	Client(std::shared_ptr<Channel> channel, std::string st_path) : stub_(RPC::NewStub(channel)) : client_map(st_path){
		
		rocksdb::Options options;
    	options.create_if_missing = true;
    	// rocksdb::Status status = rocksdb::DB::Open(options, st_path , &cs_db);
		status = rocksdb::DB::Open(options, st_path + ".trace" , &trace_db);

		// load all sc, uc to memory
		/*rocksdb::Iterator* it = cs_db->NewIterator(rocksdb::ReadOptions());
		std::string key;
		size_t counter = 0;
	  	for (it->SeekToFirst(); it->Valid(); it->Next()) {
			key = it->key().ToString(); 
			if (key[0] == 's') {
				st_mapper[key.substr(1, key.length() - 1)] = it->value().ToString();
			}
			else{
				uc_mapper[key.substr(1, key.length() - 1)] = std::stoi(it->value().ToString());		
			}
			counter++;
	  	}
		
	  	assert( it->status().ok() ); // Check for any errors found during the scan
	  	delete it;
		*/

		std::cout << "Just remind, previous keywords: "<< client_map.size() <<std::endl;
	}

    ~Client() {

		// must store 'sc' and 'uc' to disk
		/*size_t keyword_counter = 0;
		std::map<std::string, std::string>::iterator it;
		for ( it = st_mapper.begin(); it != st_mapper.end(); ++it) {
			store("s" + it->first, it->second);
			keyword_counter++;
		}
		
		std::map<std::string, size_t>::iterator ut;
		for ( ut = uc_mapper.begin(); ut != uc_mapper.end(); ++ut) {
			store("u" + ut->first, std::to_string(ut->second));
		}
		
		std::cout<< "Total keyword: " << keyword_counter <<std::endl;
		*/
		std::cout<< "Bye ~" <<std::endl;
	}
	

	/*bool store(const std::string k, const std::string v){
		
		rocksdb::Status s = cs_db->Delete(rocksdb::WriteOptions(), k);
		s = cs_db->Put(rocksdb::WriteOptions(), k, v);
		return s.ok();
	}

	bool get(const std::string k, std::string &r) {
		rocksdb::Status s = cs_db->Get(rocksdb::ReadOptions(), k, &r);
		return s.ok();
	}*/

	bool trace_store(const std::string l, const std::string e){
		rocksdb::Status s; 		
		rocksdb::WriteOptions write_option = rocksdb::WriteOptions();
		{
			std::lock_guard<std::mutex> lock(trace_map_mtx);		
			s = trace_db->Put(write_option, l, e);
		}
		return s.ok();
	}

	bool trace_get(const std::string l, std::string &r){
		rocksdb::Status s;
		{
			std::lock_guard<std::mutex> lock(trace_map_mtx);
			s = trace_db->Get(rocksdb::ReadOptions(), l, &r);
		}
		return s.ok();
	}

	void get_status(const std::string w, std::string& st, uint32_t& uc) {
		bool found;
		std::string value;
		{
			std::lock_guard<std::mutex> lock(client_map_mtx);	
			found = client_map.get(w, value); // value = st||uc
		}
		if(!found) {
			{
				std::lock_guard<std::mutex> lock(client_map_mtx);
				client_map.add(w, w + "0");
			}
		}
		else {
			st = value.substr(0, 16);
			uc = std::stoi( value.substr(16, value.length()-16) );
		}
	}

	int set_status(const std::string w, const std::string new_st, const uint32_t uc) {
		//set `w`'s new state
		bool found;
		std::string value ;
        {
		    std::lock_guard<std::mutex> lock(client_map_mtx);		
			found = client_map.get(w, value);
		}
		if(!found) {
			{
				std::lock_guard<std::mutex> lock(client_map_mtx);		
				client_map.add(w, new_st + std::to_string(uc));
			}
		}
		else{
			{
				std::lock_guard<std::mutex> lock(client_map_mtx);		
				client_map.at(w, new_st + std::to_string(uc));
			}
		}
		return 0;
	}

	std::string hash( const std::string in ) {
		byte buf[SHA256::DIGESTSIZE];
		{	
			std::lock_guard<std::mutex> lock(trace_map_mtx);
			hasher.Restart();
			hasher.CalculateDigest(buf, (byte*) (in.c_str()), in.length() );
		}
		return std::string((const char*)buf, (size_t)SHA256::DIGESTSIZE);
	}

	
	std::string gen_enc_token(const std::string token){
		// 使用padding方式将所有字符串补齐到16的整数倍长度
		std::string token_padding;
		std::string enc_token;
		try {
			
			CFB_Mode< AES >::Encryption e;
		
			e.SetKeyWithIV(k_s, AES128_KEY_LEN, iv_s, (size_t)AES::BLOCKSIZE); // so `key` and `iv` is fixed now
		
			token_padding = Util::padding(token);
	
			byte cipher_text[token_padding.length()];

			e.ProcessData(cipher_text, (byte*) token_padding.c_str(), token_padding.length());
			
			enc_token = std::string((const char*) cipher_text, token_padding.length());

		}
		catch(const CryptoPP::Exception& e)
		{
			std::cerr << "in gen_enc_token() " << e.what()<< std::endl;
			exit(1);
		}
		return enc_token;
	}

	void gen_new_st(std::string old_st, std::string& key, std::string& new_st) { // generate a new st and return back the key
		
		byte rand_key[AES128_KEY_LEN];
		
		try {
			
			AutoSeededRandomPool rnd;

			// Generate a random str
			rnd.GenerateBlock(rand_key, AES128_KEY_LEN);
		
			// key is for returning
			key = std::string((const char*)rand_key, AES128_KEY_LEN);

			CFB_Mode< AES >::Encryption e;

			e.SetKeyWithIV(rand_key, AES128_KEY_LEN, iv_s, (size_t)AES::BLOCKSIZE); 
	
			byte tmp_new_st[old_st.length()];

			e.ProcessData(tmp_new_st, (byte*) old_st.c_str(), old_st.length());
			
			new_st = std::string((const char*)tmp_new_st, old_st.length());
		} catch(const CryptoPP::Exception& e) {
			std::cerr << "in gen_new_st() " << e.what()<< std::endl;
			exit(1);
		}
		
	}

	void recover_st(std::string new_st, std::string key, std::string& old_st) {
		try {

			CFB_Mode< AES >::Decryption d;

			d.SetKeyWithIV((byte*) key.c_str(), AES128_KEY_LEN, iv_s, (size_t)AES::BLOCKSIZE); 
	
			byte tmp_old_st[new_st.length()];

			d.ProcessData(tmp_old_st, (byte*) new_st.c_str(), new_st.length());
			
			old_st = std::string((const char*)tmp_old_st, new_st.length());
		}
		catch(const CryptoPP::Exception& e)
		{
			std::cerr << "in recover_st() " << e.what()<< std::endl;
			exit(1);
		}
	}

	void verify_st() {
		std::string old_st = "0000000000000000", new_st, key;
		gen_new_st(old_st, key, new_st);
		old_st = "fuck";
		recover_st(new_st, key, old_st);
		assert(old_st.compare("0000000000000000") == 0 );
		// logger::log(logger::INFO) << old_st << std::endl;
	}

	void test_hash(){
		std::string s1 = hash("fuck");
		std::string s2 = hash("fuck");
		std::cout<< Util::str2hex(s1) + ", " + Util::str2hex(s2)  <<std::endl;
		assert(s1.compare(s2) == 0);
	}

	void gen_update_token(std::string op, std::string w, std::string ind, std::string& l, std::string& e) {
		try{
			std::string enc_token, rand_key;
			std::string old_st, new_st;
			uint32_t uc;

			std::string tw = gen_enc_token(w);

			get_st(w, old_st, uc);			
			gen_new_st(old_st, rand_key, new_st); // TODO

			l = hash( tw + new_st + "1");
			e = Util::Xor( op + ind + new_st, hash(tw + new_st + "2") );			
		}
		catch(const CryptoPP::Exception& e){
			std::cerr << "in gen_update_token() " << e.what() << std::endl;
			exit(1);
		}
	}

	UpdateRequestMessage gen_update_request(std::string op, std::string w, std::string ind, int counter){
		try{

			UpdateRequestMessage msg;
	
			std::string old_st, rand_key, new_st, l, e;
			uint32_t uc;

			// get update time of `w` for `node`
			std::string tw = gen_enc_token(w);

			old_st = get_st(w, old_st, uc);
			gen_new_st(old_st, rand_key, new_st); // TODO
			
			l = hash( tw + new_st + "1");
			e = Util::Xor( op + ind + new_st, hash(tw + new_st + "2") );
			assert((op + ind + rand_key).length() == 25);	

			msg.set_l(l);
			msg.set_e(e);
			msg.set_counter(counter);

			set_status(w, new_st, uc + 1); 

			return msg;
		}
		catch(const CryptoPP::Exception& e){
			std::cerr << "in gen_update_request() " << e.what() << std::endl;
			exit(1);
		}
	}

	UpdateRequestMessage gen_update_request(std::string op, std::string w, std::string ind, int counter, std::string& new_st){
		try{
			
				UpdateRequestMessage msg;
				
				std::string old_st, rand_key, l, e;
				uint32_t uc;
			
				// get update time of `w` for `node`
				std::string tw = gen_enc_token(w);
			
				old_st = get_st(w, old_st, uc);
				gen_new_st(old_st, rand_key, new_st); // TODO
						
				l = hash( tw + new_st + "1");
				e = Util::Xor( op + ind + new_st, hash(tw + new_st + "2") );
				assert((op + ind + rand_key).length() == 25);	
			
				msg.set_l(l);
				msg.set_e(e);
				msg.set_counter(counter);
			
				set_status(w, new_st, uc + 1); 
			
				return msg;
			}
			catch(const CryptoPP::Exception& e){
				std::cerr << "in gen_update_request() " << e.what() << std::endl;
				exit(1);
			}
	}


	void gen_search_token(std::string w, std::string& tw, std::string& st, size_t& uc) {
		try{
			// get update time of
			tw = gen_enc_token(w);
			get_status(w, st, uc);			
		}
		catch(const CryptoPP::Exception& e){
			std::cerr << "in gen_search_token() " <<e.what() << std::endl;
			exit(1);
		}
	}

// 客户端RPC通信部分

	std::string search(const std::string w) {
		std::string tw, st;
		size_t uc;

		gen_search_token(w, tw, st, uc);
		search(tw, st, uc);

		return "OK";
	}	

	std::string search(const std::string tw, const std::string st, const size_t uc) {
		// request包含 enc_token 和 st
		SearchRequestMessage request;
		if( uc == 0 ) request.set_kw(""); // TODO attentaion !
		else request.set_kw(st);
		request.set_tw(tw);
		request.set_uc(uc);

		ClientContext context;

		// 执行RPC操作，返回类型为 std::unique_ptr<ClientReaderInterface<SearchReply>>
		std::unique_ptr<ClientReaderInterface<SearchReply>> reader = stub_->search(&context, request);
		
		size_t counter = 0;
		SearchReply reply;
		while (reader->Read(&reply)) {
			// logger::log(logger::INFO) << reply.ind()<<std::endl;
			counter++;
		}

		return "OK";
	  }

	Status update(UpdateRequestMessage update) {

		ClientContext context;

		ExecuteStatus exec_status;
		// 执行RPC
		Status status = stub_->update(&context, update, &exec_status);
		assert (status.ok()); // increase_update_time(w);

		return status;
	}

	Status update(std::string op, std::string w, std::string ind) {
		ClientContext context;

		ExecuteStatus exec_status;
		// 执行RPC
		std::string l, e;
		gen_update_token(op, w, ind, l, e); // update(op, w, ind, _l, _e);
		UpdateRequestMessage update_request;
		update_request.set_l(l);
		update_request.set_e(e);


		Status status = stub_->update(&context, update_request, &exec_status);
		if(status.ok()) increase_update_time(w);// TODO
		assert(status.ok());
		return status;
	}

	Status batch_update(std::vector<UpdateRequestMessage> update_list) {
		UpdateRequestMessage request;

		ClientContext context;

		ExecuteStatus exec_status;

		std::unique_ptr<ClientWriterInterface<UpdateRequestMessage>> writer(stub_->batch_update(&context, &exec_status));
		int i = 0;		
		while(i < update_list.size()){
			writer->Write(update_list[i]);
		}
		writer->WritesDone();
	    Status status = writer->Finish();

		return status;
	}

	void test_upload( int wsize, int dsize ){
		std::string l,e;
		for(int i = 0; i < wsize; i++)
			for(int j =0; j < dsize; j++){
				gen_update_token("1", std::to_string(i), std::to_string(j), l, e); // update(op, w, ind, _l, _e);
				UpdateRequestMessage update_request;
				update_request.set_l( l );
				update_request.set_e( e );
				// logger::log(logger::INFO) << "client.test_upload(), l:" << l <<std::endl;
				Status s = update( update_request ); // TODO
				// if (s.ok()) increase_update_time( std::to_string(i) );

				if ( (i * dsize + j) % 1000 == 0) logger::log(logger::INFO) << " updating :  "<< i * dsize + j << "\r" << std::flush;
			}
	}
};

} // namespace DistSSE

#endif // DISTSSE_CLIENT_H
