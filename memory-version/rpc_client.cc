#include "DistSSE.client.h"

#include "logger.h"

using DistSSE::SearchRequestMessage;

int main(int argc, char** argv) {
  // Instantiate the client and channel isn't authenticated
	DistSSE::Client client(grpc::CreateChannel("localhost:50051", grpc::InsecureChannelCredentials()), std::string(argv[1]));
	
	if (argc < 4) {
		std::cerr<<"argc error"<<std::endl;	
		exit(-1);
	}

	int N_entry = atoi(argv[2]);
	// int dsize = atoi(argv[3]);
	
	std::cout << "update begin..." <<std::endl;

  	// client.test_upload(wsize, dsize);
	std::atomic_int total(0);
	client.generation_job(0, N_entry, 1, &total);
	// client.test_upload(14, N_entry);	


  	std::cout <<"update done." <<std::endl;
	
	std::string w = std::string(argv[3]);
	std::string kw, tw;
	int uc;
	std::cout << " search beign:  "<< std::endl;
	client.gen_search_token(w, kw, tw, uc);
	client.search(kw, tw, uc);
	
	client.increase_search_time(w);
	client.set_update_time(w, 0);
	
	std::cout << "search done: "<< std::endl;
	
	return 0;
}

