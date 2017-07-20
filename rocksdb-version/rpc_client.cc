#include "DistSSE.client.h"
// #include "DistSSE.trace.h"
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
	std::atomic_int total(0);
	unsigned int n_threads = atoi(argv[4]);

	// double search_rate;
	// sscanf(argv[5],"%lf", &search_rate);
	// std::cout << search_rate <<std::endl;
	generate_trace(&client, N_entry);
	


  	std::cout <<"update done." <<std::endl;
	
	std::string w = std::string(argv[3]);
	std::string kw, tw;
	size_t uc;

	client.gen_search_token(w, kw, tw, uc);
	client.search(kw, tw, uc);
	
	client.increase_search_time(w);
	client.set_update_time(w, 0);
	
	std::cout << "search done: "<< std::endl;
	
	return 0;
}

