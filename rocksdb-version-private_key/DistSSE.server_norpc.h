/*
 * Created by Xiangfu Song on 10/21/2016.
 * Email: bintasong@gmail.com
 * 
 */
#ifndef DISTSSE_SERVER_NORPC_H
#define DISTSSE_SERVER_NORPC_H


#include "DistSSE.Util.h"

#include "logger.h"

#include <unordered_set>


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

class Client {

private:
	rocksdb::DB* cs_db;
	
	std::mutex st_mtx;
	std::mutex uc_mtx;

	std::map<std::string, std::string> st_mapper;	
	std::map<std::string, size_t> uc_mapper;

public:

  	Client(std::shared_ptr<Channel> channel, std::string db_path) : stub_(RPC::NewStub(channel)){
		rocksdb::Options options;
    	options.create_if_missing = true;
    	rocksdb::Status status = rocksdb::DB::Open(options, db_path , &cs_db);

		// load all sc, uc to memory
		rocksdb::Iterator* it = cs_db->NewIterator(rocksdb::ReadOptions());
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

		std::cout << "Just remind, previous keywords: "<< counter/2 <<std::endl;
	}

    ~Client() {

		// must store 'sc' and 'uc' to disk 

		size_t keyword_counter = 0;
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

		std::cout<< "Bye~ " <<std::endl;
	}
	

	int store(const std::string k, const std::string v){
		rocksdb::Status s = cs_db->Delete(rocksdb::WriteOptions(), k);
		s = cs_db->Put(rocksdb::WriteOptions(), k, v);
		if (s.ok())	return 0;
		else return -1;
		assert(s.ok());
	}

	std::string get(const std::string k) {
		std::string tmp;
		rocksdb::Status s = cs_db->Get(rocksdb::ReadOptions(), k, &tmp);
		if (s.ok())	return tmp;
		else return "";
	}

	std::string get_st(std::string w){

		std::string st;
		
		std::map<std::string, std::string>::iterator it;		
		
		it = st_mapper.find(w);
		
		if (it != st_mapper.end()) {
			st = it->second;
		}
		else {
			// std::string value = get("s" + w );

			// search_time = value == "" ? 0 : std::stoi(value);
			byte _st[AES128_KEY_LEN];
			AutoSeededRandomPool rnd;
			rnd.GenerateBlock(_st, AES128_KEY_LEN);
			st = std::string((const char*)_st, AES128_KEY_LEN);
			set_st(w, st); // cache search_time into sc_mapper 
		}
		return st;
	}

	int set_st(std::string w, std::string new_st) {
		//set `w`'s new state
        {
		    std::lock_guard<std::mutex> lock(st_mtx);		
			st_mapper[w] = new_st;
		}
		// no need to store, because ti will be done in ~Client()
		// store(w + "_search", std::to_string(search_time)); 
		return 0;
	}

	int get_update_time(std::string w) {

		int update_time;

		std::map<std::string, size_t>::iterator it;

		it = uc_mapper.find(w);
		
		if (it != uc_mapper.end()) {
			update_time = it->second;
		}
		else{
			// std::string value = get("u" + w );

			// update_time = value == "" ? 0 : std::stoi(value);
			update_time = 0;
			set_update_time(w, 0);
		}
		return update_time;
	}

	int set_update_time(std::string w, size_t update_time){
		{
			std::lock_guard<std::mutex> lock(uc_mtx);
			uc_mapper[w] = update_time;
		}		
		return 0;
	}
	
	void increase_update_time(std::string w) {
		set_update_time(w, get_update_time(w) + 1);
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

			// logger::log(logger::INFO) <<"In gen_new_st: " << new_st << std::endl; // TODO

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

	void gen_update_token(std::string op, std::string w, std::string ind, std::string& l, std::string& e) {
		try{
			std::string enc_token, rand_key;
	
			std::string tw, old_st, new_st;
			// get update time of `w` for `node`
			// std::string st;
			old_st = get_st(w);

			tw = gen_enc_token(w);
			
			gen_new_st(old_st, rand_key, new_st); // TODO

			// generating update pair, which is (l, e) 
			l = Util::H1( tw + new_st);
			// e = Util::Xor( op + ind + rand_key, Util::H2(tw + new_st) );
			e = Util::Xor( op + ind + new_st, Util::H2(tw + new_st) );
			// increase_update_time(w);
			
		}
		catch(const CryptoPP::Exception& e){
			std::cerr << "in gen_update_token() " << e.what() << std::endl;
			exit(1);
		}
	}

	UpdateRequestMessage gen_update_request(std::string op, std::string w, std::string ind, int counter){
		try{
			std::string enc_token, rand_key;
			UpdateRequestMessage msg;
	
			std::string tw, old_st, new_st, l, e;
			// get update time of `w` for `node`

			tw = gen_enc_token(w);

			old_st = get_st(w);

			gen_new_st(old_st, rand_key, new_st); // TODO
			
			// logger::log(logger::INFO) << kw <<std::endl;

			l = Util::H1( tw + new_st);
			// e = Util::Xor(op + ind + rand_key, Util::H2(tw + new_st));
			e = Util::Xor( op + ind + old_st, Util::H2(tw + new_st) );
			// logger::log(logger::INFO) <<"In gen_update_request==>  " << "st:" << new_st << ", tw: " << tw << std::endl;
			assert((op + ind + rand_key).length() == 25);			

			msg.set_l(l);
			msg.set_e(e);
			msg.set_counter(counter);

			set_st(w, new_st); // TODO
			increase_update_time(w);

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
			
			st = get_st(w);

			uc = get_update_time(w);

			// logger::log(logger::INFO) <<"In gen_search_token==>  " << "st:" << st << ", tw: " << tw <<", uc:"<< uc <<std::endl;
			
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
		if( uc == 0 ) request.set_kw(""); // TODO attentaion here !!!
		else request.set_kw(st);
		request.set_tw(tw);
		request.set_uc(uc);

		// Context for the client. It could be used to convey extra information to the server and/or tweak certain RPC behaviors.
		ClientContext context;

		// 执行RPC操作，返回类型为 std::unique_ptr<ClientReaderInterface<SearchReply>>
		std::unique_ptr<ClientReaderInterface<SearchReply>> reader = stub_->search(&context, request);
		
		// 读取返回列表
		int counter = 0;
		SearchReply reply;
		while (reader->Read(&reply)){
			// logger::log(logger::INFO) << reply.ind()<<std::endl;
			counter++;
		}
		// logger::log(logger::INFO) << " search result: "<< counter << std::endl;
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
		if(status.ok()) increase_update_time(w); // TODO
		assert(status.ok());
		return status;
	}

};


class DistSSEServiceImpl {

private:	
	static rocksdb::DB* ss_db;
	rocksdb::DB* cache_db;
    int MAX_THREADS;

	rocksdb::Options options;

	int old_miss, new_miss;
	int old_hit, new_hit;

	// static std::mutex ssdb_write_mtx;

public:
	DistSSEServiceImpl(const std::string db_path, const std::string cache_path, int concurrent){
		signal(SIGINT, abort);

    	options.create_if_missing = true;

	    Util::set_db_common_options(options);

		rocksdb::Status s1 = rocksdb::DB::Open(options, db_path, &ss_db);

		// assert(s1.ok());

		MAX_THREADS = concurrent; //std::thread::hardware_concurrency();

	old_miss = 0;
	new_miss = 0;
	old_hit = 0;
	new_hit = 0;

	}

	static void abort( int signum )
	{
		delete ss_db;
		// delete cache_db; 
		logger::log(logger::INFO)<< "abort: "<< signum <<std::endl;
	   	exit(signum);
	}

	static int store(rocksdb::DB* &db, const std::string l, const std::string e){
		rocksdb::Status s; 		
		rocksdb::WriteOptions write_option = rocksdb::WriteOptions();
		//write_option.sync = true;
		//write_option.disableWAL = false;
		{
			// std::lock_guard<std::mutex> lock(ssdb_write_mtx);		
			s = db->Put(write_option, l, e);
			// db->SyncWAL();
		}
		if (s.ok())	return 0;
		else return -1;
	}

	static std::string get(rocksdb::DB* &db, const std::string l){
		std::string tmp;
		rocksdb::Status s;
		{
			// std::lock_guard<std::mutex> lock(ssdb_write_mtx);
			s = db->Get(rocksdb::ReadOptions(), l, &tmp);
		}
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

	// only used for expriment measurement
	static void search_log(std::string kw, double search_time, double get_time, int result_size) { 
		// std::ofstream out( "search.slog", std::ios::out|std::ios::app);
		byte k_s[17] = "0123456789abcdef";
		byte iv_s[17] = "0123456789abcdef";

		std::string keyword = Util::dec_token(k_s, AES128_KEY_LEN, iv_s, kw);
			
		std::string word = keyword == "" ? "cached" : keyword;
		
		std::cout <<  word + "\t" + std::to_string(result_size)+ "\t" + std::to_string(get_time) + "\t" + std::to_string(search_time) + "\t" + std::to_string(search_time/	result_size) << std::endl;

	}


	void search (std::string tw, std::string st, size_t uc, std::unordered_set<std::string>& ID){
	
		std::vector<std::string> op_ind;

		std::string op, ind, rand_key;
		std::string _st, l, e, value;
		// std::string cache_ind;

		int counter = 0;

		struct timeval t1, t2, t3, t4;

		gettimeofday(&t1, NULL);


		std::unordered_set<std::string> result_set;
		std::unordered_set<std::string> delete_set;
	    _st = st;
		
		//logger::log(logger::INFO) << "server searching... "<< uc <<std::endl;
		// logger::log(logger::INFO) <<"In gen_search_token==>  " << "st:" << st << ", tw: " << tw << ", uc: "<< uc <<std::endl;
		
		int repeat;
		double get_time = 0.0;

		for(size_t i = 1; i <= uc; i++) {
			repeat = 0;
//logger::log(logger::INFO) << "fuck-------------- "<< i << value <<std::endl;
			l = Util::H1(tw + _st);
			
			gettimeofday(&t3, NULL);

			e = get(ss_db, l);

			gettimeofday(&t4, NULL);

get_time +=  ((t4.tv_sec - t3.tv_sec) * 1000000.0 + t4.tv_usec - t3.tv_usec) /1000.0;
			
			assert(e != "");

			value = Util::Xor( e, Util::H2(tw + _st) );

//logger::log(logger::INFO) << "value: "<< value <<std::endl;
			// logger::log(logger::INFO) << "value: "<< value <<std::endl;
			//ID.insert( value );

			// parse(value, op, ind, rand_key); 

            parse(value, op, ind, _st); // At present, |st| = |key|, so we just store st too prevent envole P^-1(st_i)

			/*if(op == "1")*/ result_set.insert(ind); // TODO
//logger::log(logger::INFO) << "hi-------------- "<< value <<std::endl;
			//else result_set->erase(ind);
			
			// remove or add 
			/*if (op == "0") {
				delete_set.insert(ind);		
			}
			else if(op == "1") {
				std::set<std::string>::iterator it = delete_set.find(ind);
				if (it != delete_set.end() ) {
					delete_set.erase(ind);				
				}else{
					result_set.insert(ind);				
				}
			}*/

			// recover_st( _st, rand_key, _st );
		}

		gettimeofday(&t2, NULL);

		double search_time =  ((t2.tv_sec - t1.tv_sec) * 1000000.0 + t2.tv_usec - t1.tv_usec) /1000.0;
		
		//new_miss = options.statistics->getTickerCount(0);
		//new_hit = options.statistics->getTickerCount(1);
		 
		search_log(tw, search_time, get_time);
		
		old_miss = new_miss;
		old_hit = new_hit;
		
		//std::cout<< options.statistics->ToString() << std::endl;
		
		std::string ID_string = "";
		for (std::unordered_set<std::string>::iterator it = ID.begin(); it != ID.end(); ++it){
    		ID_string += Util::str2hex(*it) + "|";
		}
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
		
		std::unordered_set<std::string> ID;

		// logger::log(logger::INFO) << "searching... " <<std::endl;

		// gettimeofday(&t1, NULL);
		search(tw, st, uc, ID);
		// gettimeofday(&t2, NULL);

  		//logger::log(logger::INFO) <<"ID.size():"<< ID.size() <<" ,search time: "<< ((t2.tv_sec - t1.tv_sec) * 1000000.0 + t2.tv_usec - t1.tv_usec) /1000.0/ID.size()<<" ms" <<std::endl;
		// TODO 读取之后需要解锁

	/*	SearchReply reply;
		
		for(int i = 0; i < ID.size(); i++){
			reply.set_ind(std::to_string(i));
			writer->Write(reply);
		}
	*/
		//logger::log(logger::INFO) << "search done." <<std::endl;

	    return Status::OK;
  	}
	
	void random_put(int num) {

			AutoSeededRandomPool prng;
			int ind_len = AES::BLOCKSIZE; // AES::BLOCKSIZE = 16
			byte tmp[ind_len];


         	for(int i = 0; i < num; i++) {
				prng.GenerateBlock(tmp, sizeof(tmp));
				std::string key = (std::string((const char*)tmp, ind_len));
				std::string value = (std::string((const char*)tmp, ind_len / 2));
				ss_db->Put(rocksdb::WriteOptions(), key, value);
			}
	}

	// update()实现单次更新操作
	Status update(ServerContext* context, const UpdateRequestMessage* request, ExecuteStatus* response) {
		
		std::string l = request->l();
		std::string e = request->e();
		//logger::log(logger::INFO) <<"in update"<<std::endl;
		int status = store(ss_db, l, e);
		if(status != 0) {
			response->set_status(false);
			return Status::CANCELLED;
		}
		
		// random_put();		

		response->set_status(true);

		return Status::OK;
	}
	
	// batch_update()实现批量更新操作
	Status batch_update(ServerContext* context, ServerReader< UpdateRequestMessage >* reader, ExecuteStatus* response) {
		std::string l;
		std::string e;
		// TODO 读取数据库之前要加锁，读取之后要解锁
		UpdateRequestMessage request;
		//std::cout<< "in batch update" <<std::endl;
		while (reader->Read(&request)){
			l = request.l();
			e = request.e();
			store(ss_db, l, e);
			//std::cout<< "in batch update" <<std::endl;
		}
		// TODO 读取之后需要解锁

		response->set_status(true);
		return Status::OK;
	}
};

}// namespace DistSSE



// static member must declare out of main function !!!

rocksdb::DB* DistSSE::DistSSEServiceImpl::ss_db;
// std::mutex DistSSE::DistSSEServiceImpl::ssdb_write_mtx;


#endif // DISTSSE_SERVER_NORPC_H
