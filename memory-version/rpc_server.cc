#include "DistSSE.server.h"

int main(int argc, char *argv[]){
	if (argc < 2) {
		std::cerr<<"argc error"<<std::endl;	
		exit(-1);
	}
	RunServer(atoi(argv[1]) );
}
























