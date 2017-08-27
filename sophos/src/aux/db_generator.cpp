//
// Sophos - Forward Private Searchable Encryption
// Copyright (C) 2016 Raphael Bost
//
// This file is part of Sophos.
//
// Sophos is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// Sophos is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with Sophos.  If not, see <http://www.gnu.org/licenses/>.
//


#include "db_generator.hpp"
#include "logger.hpp"
#include <sys/time.h>

#include <sse/crypto/fpe.hpp>

#include <string>
#include <thread>
#include <vector>
#include <iostream>
#include <chrono>

namespace sse {
    namespace sophos {

        const std::string kKeyword01PercentBase    = "0.1";
        const std::string kKeyword1PercentBase     = "1";
        const std::string kKeyword10PercentBase    = "10";

        const std::string kKeywordGroupBase      = "fuck-";
        const std::string kKeyword10GroupBase    = kKeywordGroupBase + "10^";

        constexpr uint32_t max_10_counter = ~0;
 
		static bool sample(double value, double rate) {
			double _value = value;			
			_value -= rate;
			return _value < 0.000000001 ? true : false;
		}
		
		static double rand_0_to_1(){ //
			return ((double) rand() / (RAND_MAX));		
		}
		
		static void search_log(std::string word, int counter) {
		
			std::cout << word + "\t" + std::to_string(counter)<< std::endl;
		}

       
        static void generation_job(SophosClientRunner* client, unsigned int thread_id, size_t N_entries, size_t step, crypto::Fpe *rnd_perm, std::atomic_size_t *entries_counter)
        {
            size_t counter = thread_id;
            std::string id_string = std::to_string(thread_id);
            
            uint32_t counter_10_1 = 0, counter_20 = 0, counter_30 = 0, counter_60 = 0, counter_10_2 = 0, counter_10_3 = 0, counter_10_4 = 0, counter_10_5 = 0;
            
            std::string keyword_01, keyword_1, keyword_10;
            std::string kw_10_1, kw_10_2, kw_10_3, kw_10_4, kw_10_5, kw_20, kw_30, kw_60;
            
            for (size_t i = 0; counter < N_entries; counter += step, i++) {
                size_t ind = rnd_perm->encrypt_64(counter);
                
                uint32_t ind_01 = ind % 1000;
                uint32_t ind_1  = ind_01 % 100;
                uint32_t ind_10 = ind_1 % 10;
                
                keyword_01  = kKeyword01PercentBase    + "_" + std::to_string(ind_01)   + "_1";//  randomly generated
                keyword_1   = kKeyword1PercentBase     + "_" + std::to_string(ind_1)    + "_1";
                keyword_10  = kKeyword10PercentBase    + "_" + std::to_string(ind_10)   + "_1";
                
                client->async_update(keyword_01, ind);
                client->async_update(keyword_1, ind);
                client->async_update(keyword_10, ind);
                
                ind_01 = (ind/1000) % 1000;
                ind_1  = ind_01 % 100;
                ind_10 = ind_1 % 10;
                
                keyword_01  = kKeyword01PercentBase    + "_" + std::to_string(ind_01)   + "_2";
                keyword_1   = kKeyword1PercentBase     + "_" + std::to_string(ind_1)    + "_2";
                keyword_10  = kKeyword10PercentBase    + "_" + std::to_string(ind_10)   + "_2";
                
                client->async_update(keyword_01, ind);
                client->async_update(keyword_1, ind);
                client->async_update(keyword_10, ind);

                ind_01 = (ind/((unsigned int)1e6)) % 1000;
                ind_1  = ind_01 % 100;
                ind_10 = ind_1 % 10;
                
                keyword_01  = kKeyword01PercentBase    + "_" + std::to_string(ind_01)   + "_3";
                keyword_1   = kKeyword1PercentBase     + "_" + std::to_string(ind_1)    + "_3";
                keyword_10  = kKeyword10PercentBase    + "_" + std::to_string(ind_10)   + "_3";
                
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
                if (((*entries_counter) % 100000) == 0) {
                    logger::log(sse::logger::INFO) << "Random DB generation: " << (*entries_counter) << " entries generated\r" << std::flush;
                }
                
                client->async_update(kw_10_1, ind);
                client->async_update(kw_10_2, ind);
                client->async_update(kw_10_3, ind);
                client->async_update(kw_10_4, ind);
                client->async_update(kw_10_5, ind);
                client->async_update(kw_20, ind);
                client->async_update(kw_30, ind);
                client->async_update(kw_60, ind);

            }
            
            std::string log = "Random DB generation: thread " + std::to_string(thread_id) + " completed: (" + std::to_string(counter_10_1) + ", " + std::to_string(counter_10_2) + ", "+ std::to_string(counter_10_3) + ", "+ std::to_string(counter_10_4) + ", "+ std::to_string(counter_10_5) + ")";
            logger::log(logger::INFO) << log << std::endl;
        }
        
        
        void gen_db(SophosClientRunner& client, size_t N_entries)
        {
            crypto::Fpe rnd_perm;
            std::atomic_size_t entries_counter(0);

            client.start_update_session();

            unsigned int n_threads = std::thread::hardware_concurrency();
            std::vector<std::thread> threads;
            std::mutex rpc_mutex;
 
	    struct timeval t1, t2;		
            gettimeofday(&t1, NULL);           
            for (unsigned int i = 0; i < n_threads; i++) {
                threads.push_back(std::thread(generation_job, &client, i, N_entries, n_threads, &rnd_perm, &entries_counter));
            }

            for (unsigned int i = 0; i < n_threads; i++) {
                threads[i].join();
            }

            client.end_update_session();
	
	    gettimeofday(&t2, NULL);
            logger::log(logger::INFO) <<"update time: "<<((t2.tv_sec - t1.tv_sec) * 1000000.0 + t2.tv_usec -t1.tv_usec) /1000.0<<" ms" <<std::endl;        

	}

	 void generate_trace(SophosClientRunner& client, size_t N_entries) {
			// randomly generate a large db
			//gen_db(client, N_entries);
			//logger::log(logger::DBG) << "DB generation finished."<< std::endl;
				
			client.start_update_session();
			// then perform trace generation
			const std::string TraceKeywordGroupBase = "Trace-";

		//	AutoSeededRandomPool prng;
		//	int ind_len = AES::BLOCKSIZE / 2; // AES::BLOCKSIZE = 16
		//	byte tmp[ind_len];


	//		UpdateRequestMessage request;
	//		ClientContext context;
	//		ExecuteStatus exec_status;
	//		std::unique_ptr<RPC::Stub> stub_(RPC::NewStub( grpc::CreateChannel("localhost:50051", grpc::InsecureChannelCredentials()) ) );
	//		std::unique_ptr<ClientWriterInterface<UpdateRequestMessage>> writer(stub_->batch_update(&context, &exec_status));

			// generate some trash data to certain large...
			double search_rate[4] = {0.0001, 0.001, 0.01, 0.1};
			int delay[4] = {30, 40, 60};
			std::string l, e;

			bool not_repeat_search = true;
			int search_time = 0, entries_counter = 0;
			srand(123);

		std::mutex logger_mtx;
        	std::ostream& log_stream = sse::logger::log(sse::logger::INFO);
        	bool first = true;
        
		auto print_callback = [&logger_mtx, &log_stream, &first](uint64_t res)
		    {
		         return;
		    };


			crypto::Fpe rnd_perm;
	//		Status s;
			//size_t counter;//ind = rnd_perm->encrypt_64(counter);
			for(int i = 2; i >= 0; i-- ) {

				// double search_rate = search_rate[i];
				for(int j = 5; j <= 5; j++) {
					std::string keyword = TraceKeywordGroupBase + "_" + std::to_string(i) + "_" + std::to_string(j);
					
					for(int k = 0; k < pow(10, j);k++) {
						// double r = rand_0_to_1();
						// bool is_search = sample(r, search_rate[i]);
						
					 	size_t ind = rnd_perm.encrypt_64(entries_counter);
		 				entries_counter++;
						client.async_update(keyword, ind);
						if(k % 100 == 0) std::this_thread::sleep_for(std::chrono::milliseconds(delay[i]));
						
						double r = rand_0_to_1();
                                                bool is_search = sample(r, search_rate[i]);
						if (is_search) {
							// 执行搜索
							std::this_thread::sleep_for(std::chrono::milliseconds(delay[i]));
							for(int i=0; i<3; i++){
								client.search(keyword, print_callback);
							}
							search_time++ ;
							search_log(keyword, k);
						}
					}// for k	
				}//for j
			}//for i

			client.end_update_session();
			logger::log(logger::INFO) << "Trace DB generation: " << ": " << (entries_counter) << " entries generated" <<std::endl;
			

			logger::log(logger::INFO) << "search time: "<<search_time << std::endl ;			

			// now tell server we have finished
		//	writer->WritesDone();
	    	//s = writer->Finish();
		//	assert(s.ok());
		}// generate_trace
    }
}
