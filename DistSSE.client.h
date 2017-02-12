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

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReaderInterface;
using grpc::ClientWriterInterface;
using grpc::Status;

using namespace CryptoPP;

// 用来生成 kw
byte k_s[17] = "0123456789abcdef";
byte iv_s[17] = "0123456789abcdef";

// 用来生成加密 label
byte k_l[17] = "abcdef1234567890";
byte iv_l[17] = "0123456789abcdef";

// 用来生成搜索 token
byte k_st[17] = "123456789abcdef0";
byte iv_st[17] = "0abcdef123456789";

extern int max_keyword_length;
extern int max_nodes_number;

namespace DistSSE{

class Client {
private:
 	std::unique_ptr<RPC::Stub> stub_;
	rocksdb::DB* cs_db;

public:
  	Client(std::shared_ptr<Channel> channel, std::string db_path) : stub_(RPC::NewStub(channel)){
		rocksdb::Options options;
    	options.create_if_missing = true;
    	rocksdb::Status status = rocksdb::DB::Open(options, db_path, &cs_db);	
	}
	
	int route(std::string label){
		// TODO
		return 0;
	}

	int store(const std::string ut, const std::string e){
		rocksdb::Status s = cs_db->Put(rocksdb::WriteOptions(), ut, e);
		if (s.ok())	return 0;
		else return -1;
	}

	std::string get(const std::string ut){
		std::string tmp;
		rocksdb::Status s = cs_db->Get(rocksdb::ReadOptions(), ut, &tmp);
		if (s.ok())	return tmp;
		else return "";
	}

	int get_update_time(std::string w, int node, int max_nodes_number){
		// 获取单词w在节点node上的更新
		if (node >= max_nodes_number) {
			// ERROR !
			std::cout<<"node out of range!"<<std::endl;
			return -1;
		}

		int update_time;
		std::string update_str = get(w + "|" + std::to_string(node));
		if (update_str == ""){
			// 如果数据库中没有该单词的信息（之前系统中不存在该单词）
			update_time = 0;
			for(int i = 0; i < max_nodes_number; i++) {
				store(w + "|" + std::to_string(i), "0");		
			}
		}else{
			update_time = std::stoi(update_str);
		}
		return update_time;
	}

	int set_update_time(std::string w, int node, int update_time, int max_nodes_number){
		//设置单词w在节点node上的更新次数为update_time
		if (node >= max_nodes_number) {
			//ERROR
			std::cout<<"node out fo range!"<<std::endl;
			return -1;
		}
		store(w + "|" + std::to_string(node), std::to_string(update_time));
		return 0;
	}
	
	std::string gen_enc_token(const void* key, int key_len, const void* iv, const std::string token){
		// 使用padding方式将所有字符串补齐到16的整数倍长度
		std::string token_padding;
		std::string enc_token;
		try
		{
			CFB_Mode< AES >::Encryption e;
		
			e.SetKeyWithIV((byte*)key, key_len, (byte*)iv, (size_t)AES::BLOCKSIZE);
		
			token_padding = Util::padding(token);
	
			byte cipher_text[token_padding.length()];
			e.ProcessData(cipher_text, (byte*) token_padding.c_str(), token_padding.length());
			enc_token = std::string((const char*)cipher_text, token_padding.length());

		}
		catch(const CryptoPP::Exception& e)
		{
			std::cerr << "in gen_enc_token() " << e.what()<< std::endl;
			exit(1);
		}
		return enc_token;
	}

	void gen_update_token(std::string op, std::string w, std::string ind, std::string& ut, std::string& value){
		try{
			std::string enc_token, label, enc_label;
			int node;
	
			// generating encrypted token and encrypted label	
			enc_token = gen_enc_token(k_s, AES128_KEY_LEN, iv_s, w);
			label = ind + w;
			enc_label = gen_enc_token(k_l, AES128_KEY_LEN, iv_l, label);
			node = route(enc_label);
	
	
			std::string st1, st2;
			// get update time of `w` for `node`
			int update_time = 0;
			update_time = get_update_time(w, node, max_nodes_number);
			if (update_time == 0){
				st1 = "NULL";
				st2 = gen_enc_token(k_s, AES128_KEY_LEN, iv_s, w + "|" + std::to_string(node) + "|" + std::to_string(1) );
			}
			else{
				st1 = gen_enc_token(k_s, AES128_KEY_LEN, iv_s, w + "|" + std::to_string(node) + "|" + std::to_string(update_time) );
				st2 = gen_enc_token(k_s, AES128_KEY_LEN, iv_s, w + "|" + std::to_string(node) + "|" + std::to_string(update_time + 1) );
			}

			// generating update pair, which is (ut, e)
			ut = Util::H1(enc_token + st2);
			value = Util::Enc( st2.c_str(), st2.size(), ind + "|" + op + "|" + Util::str2hex(st1) );  //TODO
			update_time++;
			set_update_time(w, node, update_time, max_nodes_number);
		}
		catch(const CryptoPP::Exception& e){
			std::cerr << "in update() " <<e.what() << std::endl;
			exit(1);
		}
	}

	std::vector<std::string> gen_search_token(std::string word, int max_node_number){
		std::vector<std::string> token_list;	
		std::string enc_token, st;
		enc_token = gen_enc_token(k_s, AES128_KEY_LEN, iv_s, word);
		token_list.push_back(enc_token);
	
		for(int node = 0; node < max_nodes_number; node++){
			int update_time = get_update_time(word, node, max_node_number);
			if (update_time == 0)
				st = "NULL";
			else{
				st = gen_enc_token(k_s, AES128_KEY_LEN, iv_s, word + "|" + std::to_string(node) + "|" + std::to_string(update_time));		
			}
			token_list.push_back(st);
		}
		return token_list;
	}

// 客户端RPC通信部分

	std::string search(const std::string search_token, const std::string st) {
		// request包含 enc_token 和 st
		SearchRequestMessage request;
		request.set_enc_token(search_token);
		request.set_st(st);

		// Context for the client. It could be used to convey extra information to the server and/or tweak certain RPC behaviors.
		ClientContext context;

		// 执行RPC操作，返回类型为 std::unique_ptr<ClientReaderInterface<SearchReply>>
		std::unique_ptr<ClientReaderInterface<SearchReply>> reader = stub_->search(&context, request);
		
		// 读取返回列表
		/*SearchReply reply;
		while (reader->Read(&reply)) {
		  logger::log(logger::INFO) << reply.ind()<<std::endl;
		}*/
		logger::log(logger::INFO) << " search done:  "<< std::endl;
		return "OK";
	  }


	Status update(UpdateRequestMessage update) {

		ClientContext context;

		ExecuteStatus exec_status;
		// 执行RPC
		Status status = stub_->update(&context, update, &exec_status);

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
		std::string ut,e;
		for(int i = 0; i < wsize; i++)
			for(int j =0; j < dsize; j++){
				gen_update_token("ADD", std::to_string(i), std::to_string(j), ut, e); // update(op, w, ind, _ut, _e);
				UpdateRequestMessage update_request;
				update_request.set_ut(ut);
				update_request.set_enc_value(e);
				update(update_request);
				if ( (i * dsize + j) % 1000 == 0) logger::log(logger::INFO) << " updating :  "<< i * dsize + j << "\r" << std::flush;
			}
	}

};

} // namespace DistSSE

#endif // DISTSSE_CLIENT_H
