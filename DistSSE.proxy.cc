#include "DistSSE.proxy.h"


int main(int argc, char *argv[]){

	// all storage nodes' IP addr
	std::vector<std::string> nodeIPVector;
	nodeIPVector.push_back("0.0.0.0:50052");
/*  nodeIPVector.push_back("0.0.0.0:50053");
	nodeIPVector.push_back("0.0.0.0:50054");
	nodeIPVector.push_back("0.0.0.0:50055");
	nodeIPVector.push_back("0.0.0.0:50056");
	nodeIPVector.push_back("0.0.0.0:50057");
*/
	RunProxy(nodeIPVector);
}
























