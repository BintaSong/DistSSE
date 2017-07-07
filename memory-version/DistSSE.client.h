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
	

	int store(const std::string k, const std::string v){
		rocksdb::Status s = cs_db->Put(rocksdb::WriteOptions(), k, v);
		if (s.ok())	return 0;
		else return -1;
	}

	std::string get(const std::string k){
		std::string tmp;
		rocksdb::Status s = cs_db->Get(rocksdb::ReadOptions(), k, &tmp);
		if (s.ok())	return tmp;
		else return "";
	}

	int get_search_time(std::string w){

		int search_time;
		std::string value = get(w + "_search");
		if (value == ""){
			// 如果数据库中没有该单词的信息（之前系统中不存在该单词）
			search_time = 0;
		}else{
			search_time = std::stoi(value);
		}
		return search_time;
	}

	int set_search_time(std::string w, int search_time){
		//设置单词w更新次数为update_time
		store(w + "_search", std::to_string(search_time));
		return 0;
	}

	void increase_search_time(std::string w) {
		set_search_time(w, get_search_time(w) + 1);
	}

	int get_update_time(std::string w){

		int update_time;
		std::string value = get(w + "_update");
		if (value == "") {
			// 如果数据库中没有该单词的信息（之前系统中不存在该单词）
			update_time = 0;
		}else {
			update_time = std::stoi(value);
		}
		return update_time;
	}

	int set_update_time(std::string w, int update_time){
		//设置单词w更新次数为update_time
		store(w + "_update", std::to_string(update_time));
		return 0;
	}
	
	void increase_update_time(std::string w) {
		set_update_time(w, get_update_time(w) + 1);
	}

	std::string rnd_permutate(CFB_Mode< AES >::Encryption* e, std::string in_64) {
		
		std::string in_64_padding, out_64;
		try {			
		
			in_64_padding = Util::padding(in_64);
	
			byte cipher_text[in_64_padding.length()];
			e->ProcessData(cipher_text, (byte*) in_64_padding.c_str(), in_64_padding.length());
			out_64 = std::string((const char*)cipher_text, in_64_padding.length());

		}
		catch(const CryptoPP::Exception& e)
		{
			std::cerr << "in rnd_permutate() " << e.what()<< std::endl;
			exit(1);
		}
		return out_64;
	}

	std::string gen_enc_token(const void* key, int key_len, const void* iv, const std::string token){
		// 使用padding方式将所有字符串补齐到16的整数倍长度
		std::string token_padding;
		std::string enc_token;
		try {
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

	void gen_update_token(std::string op, std::string w, std::string ind, std::string& l, std::string& e){
		try{
			std::string enc_token;
	
			std::string kw, tw;
			// get update time of `w` for `node`
			int sc, uc;
			uc = get_update_time(w);
			sc = get_search_time(w);


			tw = gen_enc_token(k_s, AES128_KEY_LEN, iv_s, w + "|" + std::to_string(-1) );
			kw = gen_enc_token(k_s, AES128_KEY_LEN, iv_s, w + "|" + std::to_string(sc) );

			// generating update pair, which is (l, e)
			l = Util::H1( kw + std::to_string(uc + 1) );
			e = Util::Xor( op + "|" + ind, Util::H2(kw + std::to_string(uc + 1)) );
			// increase_update_time(w);
			
		}
		catch(const CryptoPP::Exception& e){
			std::cerr << "in gen_update_token() " << e.what() << std::endl;
			exit(1);
		}
	}

	void gen_search_token(std::string w, std::string& kw, std::string& tw, int& uc) {
		try{
			// get update time of
			int sc;
			uc = get_update_time(w);
			sc = get_search_time(w);

			tw = gen_enc_token(k_s, AES128_KEY_LEN, iv_s, w + "|" + std::to_string(-1) );
			if(uc != 0)	kw = gen_enc_token(k_s, AES128_KEY_LEN, iv_s, w + "|" + std::to_string(sc) );
			else kw = "";
		    // std::cout << "kw: " << kw << ", tw: "<< tw<< ", uc: "<< uc << std::endl;
			
		}
		catch(const CryptoPP::Exception& e){
			std::cerr << "in gen_search_token() " <<e.what() << std::endl;
			exit(1);
		}
	}

// 客户端RPC通信部分

	std::string search(const std::string kw, const std::string tw, int uc) {
		// request包含 enc_token 和 st
		SearchRequestMessage request;
		request.set_kw(kw);
		request.set_tw(tw);
		request.set_uc(uc);

		// Context for the client. It could be used to convey extra information to the server and/or tweak certain RPC behaviors.
		ClientContext context;

		// 执行RPC操作，返回类型为 std::unique_ptr<ClientReaderInterface<SearchReply>>
		std::unique_ptr<ClientReaderInterface<SearchReply>> reader = stub_->search(&context, request);
		
		// 读取返回列表
		SearchReply reply;
		while (reader->Read(&reply)){
		  logger::log(logger::INFO) << reply.ind()<<std::endl;
		}
		logger::log(logger::INFO) << " search done:  "<< std::endl;
		return "OK";
	  }

	Status update(UpdateRequestMessage update) {

		ClientContext context;

		ExecuteStatus exec_status;
		// 执行RPC
		Status status = stub_->update(&context, update, &exec_status);
		// if(status.ok()) increase_update_time(w);

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
		if(status.ok()) increase_update_time(w);

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
				gen_update_token("ADD", std::to_string(i), std::to_string(j), l, e); // update(op, w, ind, _l, _e);
				UpdateRequestMessage update_request;
				update_request.set_l( l );
				update_request.set_e( e );
				// logger::log(logger::INFO) << "client.test_upload(), l:" << l <<std::endl;
				Status s = update( update_request ); // TODO
				if (s.ok()) increase_update_time( std::to_string(i) );

				if ( (i * dsize + j) % 1000 == 0) logger::log(logger::INFO) << " updating :  "<< i * dsize + j << "\r" << std::flush;
			}
	}

	void generation_job(unsigned int thread_id, size_t N_entries, size_t step, std::atomic_int* entries_counter) {
		const std::string kKeyword01PercentBase    = "0.1";
        const std::string kKeyword1PercentBase     = "1";
        const std::string kKeyword10PercentBase    = "10";

        const std::string kKeywordGroupBase      = "Group-";
        const std::string kKeyword10GroupBase    = kKeywordGroupBase + "10^";

        constexpr uint32_t max_10_counter = ~0;
        size_t counter = thread_id;
        std::string id_string = std::to_string(thread_id);
            
        uint32_t counter_10_1 = 0, counter_20 = 0, counter_30 = 0, counter_60 = 0, counter_10_2 = 0, counter_10_3 = 0, counter_10_4 = 0,      	               counter_10_5 = 0;
            
        std::string keyword_01, keyword_1, keyword_10;
        std::string kw_10_1, kw_10_2, kw_10_3, kw_10_4, kw_10_5, kw_20, kw_30, kw_60;
        
		AutoSeededRandomPool prng;
		int ind_len = AES::BLOCKSIZE - 4;
		byte tmp[ind_len];

        for (size_t i = 0; counter < N_entries; counter += step, i++) {

			prng.GenerateBlock(tmp, sizeof(tmp));
			std::string ind = std::string((const char*)tmp, ind_len);
            
            std::string ind_01 = std::string((const char*)tmp, 3);
            std::string ind_1  = std::string((const char*)tmp + 3, 1);
            std::string ind_10 = std::string((const char*)tmp + 4, 1);
                
            keyword_01  = kKeyword01PercentBase    + "_" + ind_01   + "_1";//  randomly generated
            keyword_1   = kKeyword1PercentBase     + "_" + ind_1    + "_1";
            keyword_10  = kKeyword10PercentBase    + "_" + ind_10   + "_1";
            
			// logger::log(logger::INFO) << "k_01: " << keyword_01 << std::endl;

            this->update("ADD", keyword_01, ind);
            this->update("ADD", keyword_1, ind);
            this->update("ADD", keyword_10, ind);
                
            ind_01 = std::string((const char*)tmp + 5, 3);
            ind_1  = std::string((const char*)tmp + 8, 1);
            ind_10 = std::string((const char*)tmp + 9, 1);
                
            keyword_01  = kKeyword01PercentBase    + "_" + ind_01   + "_2";
            keyword_1   = kKeyword1PercentBase     + "_" + ind_1    + "_2";
            keyword_10  = kKeyword10PercentBase    + "_" + ind_10   + "_2";
                
            this->update("ADD", keyword_01, ind);
            this->update("ADD", keyword_1, ind);
            this->update("ADD", keyword_10, ind);

//            ind_01 = (ind/((unsigned int)1e6)) % 1000;
//            ind_1  = ind_01 % 100;
//            ind_10 = ind_1 % 10;
                
//            keyword_01  = kKeyword01PercentBase    + "_" + std::to_string(ind_01)   + "_3";
//            keyword_1   = kKeyword1PercentBase     + "_" + std::to_string(ind_1)    + "_3";
//            keyword_10  = kKeyword10PercentBase    + "_" + std::to_string(ind_10)   + "_3";
                
//                client->async_update(keyword_01, ind);
//                client->async_update(keyword_1, ind);
//                client->async_update(keyword_10, ind);

                
            if (counter_10_1 < max_10_counter) {
                kw_10_1 = kKeyword10GroupBase  + "1_" + id_string + "_" + std::to_string(counter_10_1);
                  
		        if((i+1)%10 == 0)
		        {
		        	if (logger::severity() <= logger::DBG) {
		        	    logger::log(logger::DBG) << "Random DB generation: completed keyword: " << kw_10_1 << std::endl;
		            }
		            counter_10_1++;
		        }
            }
            if (counter_20 < max_10_counter) {
                kw_20 = kKeywordGroupBase  + "20_" + id_string + "_" + std::to_string(counter_20);
                
                if((i+1)%20 == 0)
                {
                    if (logger::severity() <= logger::DBG) {
                        logger::log(logger::DBG) << "Random DB generation: completed keyword: " << kw_20 << std::endl;
                    }
                        counter_20++;
                }
            }
            if (counter_30 < max_10_counter) {
                kw_30 = kKeywordGroupBase  + "30_" + id_string + "_" + std::to_string(counter_30);
                    
                if((i+1)%30 == 0)
                {
                    if (logger::severity() <= logger::DBG) {
                        logger::log(logger::DBG) << "Random DB generation: completed keyword: " << kw_30 << std::endl;
                    }
                    counter_30++;
                }
            }
            if (counter_60 < max_10_counter) {
                kw_60 = kKeywordGroupBase  + "60_" + id_string + "_" + std::to_string(counter_60);
                    
                if((i+1)%60 == 0)
                {
                    if (logger::severity() <= logger::DBG) {
                        logger::log(logger::DBG) << "Random DB generation: completed keyword: " << kw_60 << std::endl;
                    }
                    counter_60++;
                }
            }
            if (counter_10_2 < max_10_counter) {
                kw_10_2 = kKeyword10GroupBase  + "2_" + id_string + "_" + std::to_string(counter_10_2);

                if((i+1)%100 == 0)
                {
                    if (logger::severity() <= logger::DBG) {
                        logger::log(logger::DBG) << "Random DB generation: completed keyword: " << kw_10_2 << std::endl;
                    }
                    counter_10_2++;
                }
            }
            if (counter_10_3 < max_10_counter) {
                kw_10_3 = kKeyword10GroupBase  + "3_" + id_string + "_" + std::to_string(counter_10_3);

                if((i+1)%((size_t)(1e3)) == 0)
                {
                    if (logger::severity() <= logger::DBG) {
                        logger::log(logger::DBG) << "Random DB generation: completed keyword: " << kw_10_3 << std::endl;
                    }
                    counter_10_3++;
                }
            }
            if (counter_10_4 < max_10_counter) {
                kw_10_4 = kKeyword10GroupBase  + "4_" + id_string + "_" + std::to_string(counter_10_4);
                    
                if((i+1)%((size_t)(1e4)) == 0)
                {
                    if (logger::severity() <= logger::DBG) {
                        logger::log(logger::DBG) << "Random DB generation: completed keyword: " << kw_10_4 << std::endl;
                    }
                    counter_10_4++;
                }
            }
            if (counter_10_5 < max_10_counter) {
                kw_10_5 = kKeyword10GroupBase  + "5_" + id_string + "_" + std::to_string(counter_10_5);
                   
                if((i+1)%((size_t)(1e5)) == 0)
                {
                    if (logger::severity() <= logger::DBG) {
                        logger::log(logger::DBG) << "Random DB generation: completed keyword: " << kw_10_5 << std::endl;
                    }
                    counter_10_5++;
                }
            }
                
            (*entries_counter)++;
            if (((*entries_counter) % 100) == 0) {
                logger::log(logger::INFO) << "Random DB generation: " << (*entries_counter) << " entries generated\r" << std::flush;
            }
                
            this->update("ADD", kw_10_1, ind);
            this->update("ADD", kw_10_2, ind);
            this->update("ADD", kw_10_3, ind);
            this->update("ADD", kw_10_4, ind);
            this->update("ADD", kw_10_5, ind);
            this->update("ADD", kw_20, ind);
            this->update("ADD", kw_30, ind);
            this->update("ADD", kw_60, ind);

        }
            
        std::string log = "Random DB generation: thread " + std::to_string(thread_id) + " completed: (" + std::to_string(counter_10_1) + ", " 
                        + std::to_string(counter_10_2) + ", "+ std::to_string(counter_10_3) + ", "+ std::to_string(counter_10_4) + ", "
                        + std::to_string(counter_10_5) + ")";
        logger::log(logger::INFO) << log << std::endl;
        }

};

} // namespace DistSSE

#endif // DISTSSE_CLIENT_H
