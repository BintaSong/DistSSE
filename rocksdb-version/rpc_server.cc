#include "DistSSE.server.h"

int main(int argc, char *argv[]){
	if (argc < 4) {
		std::cerr<<"argc error"<<std::endl;	
		exit(-1);
	}
	RunServer(std::string(argv[1]), std::string(argv[2]), atoi(argv[3]) );
}
























