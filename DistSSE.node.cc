#include "DistSSE.node.h"

int main(int argc, char *argv[]){
	if (argc < 2) {
		std::cerr<<"argc error"<<std::endl;	
		exit(-1);
	}
	RunServer(std::string(argv[1]));
}
























