#include "DistSSE.client.h"
#include "DistSSE.db_generator.h"

#include "logger.h"

using DistSSE::SearchRequestMessage;

int main(int argc, char** argv) {
  // Instantiate the client and channel isn't authenticated
	DistSSE::Client client(grpc::CreateChannel("localhost:50051", grpc::InsecureChannelCredentials()), std::string(argv[1]));
	
	if (argc < 5) {
		std::cerr<<"argc error"<<std::endl;	
		exit(-1);
	}

	size_t N_entry = atoi(argv[2]);
	// int dsize = atoi(argv[3]);
	
	std::cout << "update begin..." <<std::endl;

  	// client.test_upload(wsize, dsize);
	//std::atomic_int total(0);
	//unsigned int n_threads = atoi(argv[4]);
	//gen_db(client, N_entry, n_threads);
	generate_trace(&client, N_entry);
	
  	std::cout <<"update done." <<std::endl;
	
	std::string w = std::string(argv[3]);
	std::string st = "ffff", tw;
	size_t uc;
	
	client.gen_search_token(w, tw, st, uc);
	// std::cout <<"In rpc-client==>  " << "st:" << st << ", tw: " << tw <<", uc:"<< uc <<std::endl;
	// client.search(tw, st, uc);
	
	std::cout << "search done: "<< std::endl;
	
	// client.verify_st();

	return 0;
}

