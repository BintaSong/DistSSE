#include "DistSSE.client.h"
#include "DistSSE.Util.h"

#include "logger.h"

using DistSSE::SearchRequestMessage;

int main(int argc, char** argv) {
  // Instantiate the client and channel isn't authenticated

	DistSSE::Client client(grpc::CreateChannel("127.0.0.1:50051", grpc::InsecureChannelCredentials()), std::string(argv[1]));

	
	int threads_num = atoi(argv[2]);

	std::string w;
	std::string prefix = "Trace";
	// std::cout << "fuck sse" << std::endl;

	std::cout << "trace begin!" << std::endl;
	for(int i = 0; i < threads_num; i++)
		for(int j = 0; j < 3; j++) {

			w = prefix + "_" + std::to_string(i) + "_" + std::to_string(j) + "_5";
			std::string w_st_c = client.trace_get(w);
			// DistSSE::logger::log(DistSSE::logger::INFO) << w_st_c << std::endl;
			if(w_st_c == "") { 
				// DistSSE::logger::log(DistSSE::logger::ERROR) << "no trace information!" << std::endl;		
				continue;			
			}
			std::vector<std::string> st_c_vector;
			DistSSE::Util::split(w_st_c, '+', st_c_vector);


			for(auto t : st_c_vector) {
			//	DistSSE::logger::log(DistSSE::logger::INFO) << w <<"<===>"<< t << std::endl;
				std::vector<std::string> st_c;
				DistSSE::Util::split(t, '|', st_c);
				std::string tw = client.gen_enc_token(w);
				DistSSE::logger::log(DistSSE::logger::INFO) << w <<"    "<<(st_c[0])<<","<<st_c[1]<<std::endl; 
				client.search(tw, DistSSE::Util::hex2str(st_c[0]), std::stoi(st_c[1]) );
			}
		}
	std::cout << "trace done."<< std::endl;
	
	return 0;
}

