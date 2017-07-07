#include "DistSSE.DBhandler.h"

int main() {
	
	DistSSE::DBhandler db("127.0.0.1", 6379);
	bool status =  db.put("a", "123");
	std::string value = db.get("a");
	std::cout<<status<< value <<std::endl;
	return 0;
}
