/*
 * Created by Xiangfu Song on 10/21/2016.
 * Email: bintasong@gmail.com
 * 
 */
#ifndef DISTSSE_PROXY_H
#define DISTSSE_PROXY_H

#include <grpc++/grpc++.h>

#include "DistSSE.grpc.pb.h"

#include "DistSSE.Util.h"

#include <thread>         // std::thread
#include <chrono>
#include <functional>
#include <atomic>


#include "logger.h"

// client side for nodeRPC
using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReaderInterface;
using grpc::ClientWriterInterface;
using grpc::ClientReaderWriterInterface;

//but as server for proxyRPC
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerWriter;
using grpc::ServerReaderWriter;
using grpc::Status;

namespace DistSSE{

class DistSSEProxyServiceImpl final : public proxyRPC::Service {
public:
	 static std::vector< std::string > ipVector; // IP for storage nodes
	 static std::vector< std::unique_ptr<nodeRPC::Stub> > stubVector; // stub for storage nodes

public:
	DistSSEProxyServiceImpl(){
	}

	~DistSSEProxyServiceImpl() {
	}
	
	void static search_task(int nodeID, SearchRequestMessage* request, std::vector<SearchReply>* resultVector) {
		
		// std::shared_ptr<Channel> channel = grpc::CreateChannel( ipVector[nodeID], grpc::InsecureChannelCredentials() );

		// std::unique_ptr<nodeRPC::Stub> stub = nodeRPC::NewStub(channel);
		
		ClientContext context;
		std::unique_ptr< ClientReaderInterface<SearchReply>> reader = stubVector[nodeID]->search(&context, *request);
		
		// get all IDs
		SearchReply reply;
		while (reader->Read(&reply)) {
			resultVector->push_back(reply);
		}
	}
	
	void merge( std::vector<SearchReply>* searchResult, int nodesCount, std::vector<SearchReply> &mergeResult){
		for (int i = 0; i < nodesCount; i++){
			for (auto& t : searchResult[i]){
				mergeResult.push_back(t);
			}
		}
	}	

// proxy's server side RPC
	// search() 实现搜索操作
	Status search(ServerContext* context, ServerReaderWriter<SearchReply, SearchRequestMessage>* stream) {
		
		SearchRequestMessage request;
		SearchReply reply;

		std::vector<SearchRequestMessage> requestVector;
		std::vector<SearchReply> replyVector;

		struct timeval t1, t2;
		
		int i = 0;
		while (stream->Read(&request)){
			requestVector.push_back(request);
		}
		
		gettimeofday(&t1, NULL);
	
		// TODO 开启多个线程，调用和存储节点交互的 nodeRPC（输入检索token，返回检索列表)
		int thread_count = ipVector.size();

		std::vector<SearchReply>* returnReply = new std::vector<SearchReply>[thread_count]; // result ID lists for storage nodes

		std::vector<std::thread> threads;
        for (int i = 0; i < thread_count; i++) {
        	threads.push_back( std::thread( search_task, i, &(requestVector[i]), &(returnReply[i]) ) );
		}
		// join theads
    	for (auto& t: threads) {
        	t.join();
    	}

		std::cout<< "return size: " <<returnReply[0].size() <<std::endl;
		// TODO 将所有检索结果进行merge
		std::vector<SearchReply> mergeResult;
		merge(returnReply, thread_count, mergeResult);
		
		gettimeofday(&t2, NULL);		
		logger::log(logger::INFO) <<"search time: "<< ((t2.tv_sec - t1.tv_sec) * 1000000.0 + t2.tv_usec - t1.tv_usec) /1000.0/mergeResult.size()<<" ms" <<std::endl;

		// TODO 返回merge之后的所有检索结果
		for (auto& t : mergeResult){
			stream->Write(t);
		}
		
		//delete returnReply;
		DistSSE::logger::log(DistSSE::logger::INFO) << "Search done." << std::endl;

	    return Status::OK;
  	}
	
	// update()实现单次更新操作
	Status update(ServerContext* server_context, const UpdateRequestMessage* request, ExecuteStatus* response) {

		int nodeID = request->node();
		std::string ut = request->ut();
		std::string enc_value = request->enc_value();

		/*
		UpdateRequestMessage newRequest;
		newRequest.set_node(nodeID);
		newRequest.set_ut(ut);
		newRequest.set_enc_value(enc_value);
		*/
		// TODO 将本次更新请求路由到指定节点

		ClientContext client_context;

		// 执行nodeRPC 的客户端操作！
		// std::unique_ptr<nodeRPC::Stub> stub = nodeRPC::NewStub(grpc::CreateChannel( ipVector[nodeID], grpc::InsecureChannelCredentials() ));

		Status status = stubVector[nodeID]->update(&client_context, *request , response); // notice *request here !!!
		// logger::log(logger::INFO) << "Update to node: "<< nodeID<< ", Success : "<<status.ok() << std::endl;
		return Status::OK;
	}
	
	// batch_update()实现批量更新操作
	Status batch_update(ServerContext* context, ServerReader< UpdateRequestMessage >* reader, ExecuteStatus* response) {
		std::string ut;
		std::string enc_value;
		// TODO 读取数据库之前要加锁，读取之后要解锁
		UpdateRequestMessage request;

		// TODO 读取之后需要解锁

		response->set_status(true);
		return Status::OK;
	}
};

}// namespace DistSSE



// static member must declare out of main function !!!
std::vector<std::string> DistSSE::DistSSEProxyServiceImpl::ipVector;
std::vector<std::unique_ptr<DistSSE::nodeRPC::Stub>> DistSSE::DistSSEProxyServiceImpl::stubVector; 

void RunProxy(std::vector<std::string> nodeIPVector) {
  	std::string server_address("0.0.0.0:50051");
// init nodes ip list
    DistSSE::DistSSEProxyServiceImpl::ipVector = nodeIPVector;

// init nodes stub list
	for(auto& t : nodeIPVector) {
		std::shared_ptr<Channel> channel = grpc::CreateChannel( t, grpc::InsecureChannelCredentials() );
		std::unique_ptr<DistSSE::nodeRPC::Stub> stub = DistSSE::nodeRPC::NewStub(channel);
		DistSSE::DistSSEProxyServiceImpl::stubVector.push_back( std::move(stub) ); // std::unique_ptr can't be moved	
	}


	for(auto& t:DistSSE::DistSSEProxyServiceImpl::ipVector){
		std::cout<<t<<std::endl;
	}

  	DistSSE::DistSSEProxyServiceImpl service;

  	ServerBuilder builder;
  	builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());

  	builder.RegisterService(&service);

  	std::unique_ptr<Server> server(builder.BuildAndStart());
  	DistSSE::logger::log(DistSSE::logger::INFO) << "Server listening on " << server_address << std::endl;

  	server->Wait();
}

#endif // DISTSSE_PROXY_H
