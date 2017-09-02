#include "DistSSE.client.h"
#include "DistSSE.db_generator.h"

#include "logger.h"

using DistSSE::SearchRequestMessage;

int main(int argc, char** argv) {

	DistSSE::Client client(grpc::CreateChannel("127.0.0.1:50051", grpc::InsecureChannelCredentials()), std::string(argv[1]));
	
	if (argc < 4) {
		std::cerr<<"argc error"<<std::endl;	
		exit(-1);
	}

	size_t N_entry = atoi(argv[2]);
	
	int threads_num = atoi(argv[3]);	

	std::cout << "gen_db_with_trace begin..." <<std::endl;
	
	gen_db_with_trace(client, N_entry, threads_num);
	
	std::cout <<"gen_db_with_trace done." <<std::endl;
	
	return 0;
}