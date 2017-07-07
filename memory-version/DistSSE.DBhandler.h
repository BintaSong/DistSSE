#ifndef DISTSSE_DBhandler_H
#define DISTSSE_DBhandler_H

#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <string>


#include <cpp_redis/cpp_redis>


namespace DistSSE{

	class DBhandler{
	private:
		cpp_redis::redis_client* client;
	public:
		DBhandler(const std::string ip, unsigned int port) {
			client = new cpp_redis::redis_client;
			client->connect(ip, port, [](cpp_redis::redis_client&) {
    			std::cout << "client disconnected (disconnection handler)" << std::endl;
  			});
		}

		~DBhandler(){
			delete client;		
		}
		
		bool put(const std::string key, const std::string value) {
			bool status = false;
			client->set(key, value, [&](cpp_redis::reply& reply) {
				// std::cout << ": " << reply << std::endl;
				if (reply.ok()) status = true;
			});

			client->sync_commit();

			return status;
		}
		
		std::string get(const std::string key) {
			std::string value = "";

			client->get(key, [&](cpp_redis::reply& reply) {
				// std::cout << "get: " << reply << std::endl;
				if(reply.ok() && !reply.is_null()) value = reply.as_string();
			});
			
			client->sync_commit();

			return value;
		}
	};// DBhandler

}//namespace DistSSE

#endif
