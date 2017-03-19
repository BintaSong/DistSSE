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

	int wsize = atoi(argv[2]);
	int dsize = atoi(argv[3]);
	
	std::cout << "update begin..." <<std::endl;

  	client.test_upload(wsize, dsize);
	
  	std::cout <<"update done." <<std::endl;
	


	std::vector<std::string> token_list = client.gen_search_token("0", max_nodes_number);
	std::cout <<"search token: "<<(token_list[0])<<"\n\n"<<std::endl;
	

	std::set<std::string> ind_set;

	SearchRequestMessage request;
	request.set_enc_token(token_list[0]);
	request.set_st(token_list[1]);

	std::vector<SearchRequestMessage> requestVector ;
    requestVector.push_back(request);

	client.search( requestVector );
	std::cout << "search done: "<< std::endl;

	return 0;
}

