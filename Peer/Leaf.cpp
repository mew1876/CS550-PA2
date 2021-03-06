#include "rpc/server.h"
#include "rpc/client.h"
#include "rpc/this_handler.h"
#include "rpc/rpc_error.h"
#include <iostream>
#include <fstream>
#include <array>
#include <vector>
#include <unordered_set>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <thread>

void queryHit(int sender, std::array<int, 2> messageId, int TTL, std::string fileName, std::vector<int> leaves);
void copyFile(int sourceId, std::string fileName);
std::vector<uint8_t> obtain(std::string fileName);
rpc::client* getClient(int clientId);
void start();
void end();
std::string getPath();

int id, superId, nSupers, startTTL;
int nextMessageId = 0;
int pendingQueries = 0;
std::unordered_set<std::string> retrievedFiles;
std::unordered_map<int, rpc::client*> leafClients;

bool canStart = false, canEnd = false;
std::mutex waitLock;
std::mutex queryCount;
std::mutex clientsLock;
std::condition_variable ready;

int main(int argc, char* argv[]) {
	//Parse args for ID, files to start with, files to request
	if (argc < 4) {
		return -1;
	}
	id = std::stoi(argv[0]);
	superId = std::stoi(argv[1]);
	nSupers = std::stoi(argv[2]);
	startTTL = std::stoi(argv[3]);
	std::cout << "Im a leaf with ID " << id << " and my super's ID is " << superId << std::endl;
	//Start server for start, obtain, and end signals
	rpc::server server(8000 + id);
	server.bind("start", &start);
	server.bind("queryHit", &queryHit);
	server.bind("obtain", &obtain);
	server.bind("end", &end);
	server.async_run(4);
	//Create super client
	rpc::client *superClient = new rpc::client("localhost", 8000 + superId);
	superClient->set_timeout(50);
	//Ping server until it responds
	while (true) {
		try {
			std::cout << "Pinging" << std::endl;
			superClient->call("ping");
			std::cout << "Ping was successful" << std::endl;
			break;
		}
		catch (rpc::timeout &t) {
			//Ping timed out, try restarting client
			delete superClient;
			superClient = new rpc::client("localhost", 8000 + superId);
			superClient->set_timeout(50);
			t; //Silence warning
		}
	}
	superClient->clear_timeout();
	//Create init files & add to super index
	CreateDirectory("Leaves", NULL);
	CreateDirectory(getPath().c_str(), NULL);
	int argIndex;
	for (argIndex = 4; argIndex < argc; argIndex++) {
		if (strcmp(argv[argIndex], std::string("requests").c_str()) == 0) {
			argIndex++;
			break;
		}
		std::string fileName(argv[argIndex]);
		std::ofstream file(getPath() + fileName);
		file << "Created by leaf " << id << std::endl;
		std::srand(unsigned int(std::time(nullptr)));
		for (int i = 0; i < argIndex * 1024; i++) {
			file << char((std::rand() % 95) + 32);
		}
		file.close();
		superClient->call("add", id, fileName);
	}
	//Send ready signal to super
	superClient->call("ready");
	//Wait for start signal
	std::unique_lock<std::mutex> unique(waitLock);
	ready.wait(unique, [] { return canStart; });
	//Make file requests
	for (; argIndex < argc; argIndex++) {
		std::string fileName(argv[argIndex]);
		std::cout << "Querying for " << fileName << std::endl;
		std::array<int, 2> messageId = { id, nextMessageId++ };
		superClient->async_call("query", id, messageId, startTTL, fileName);
		queryCount.lock();
		pendingQueries++;
		queryCount.unlock();
	}
	ready.wait(unique, [] { return pendingQueries == 0; });
	//Send complete signal to system
	rpc::client sysClient("localhost", 8000);
	sysClient.call("complete");
	delete superClient;
	//Wait for kill signal
	ready.wait(unique, [] { return canEnd; });
}

void queryHit(int sender, std::array<int, 2> messageId, int TTL, std::string fileName, std::vector<int> leaves) {
	if (retrievedFiles.find(fileName) == retrievedFiles.end()) {
		//Got the location of an unobtained file
		retrievedFiles.insert(fileName);
		try {
			int sourceId = leaves[std::rand() % leaves.size()];
			std::cout << "Sending file request to " << sourceId << " for " << fileName << std::endl;
			std::thread(copyFile, sourceId, fileName).detach();
		}
		catch (...) {
			//Error writing file
			std::cout << "Failed to download " << fileName << std::endl;
		}
	}
}

void copyFile(int sourceId, std::string fileName) {
	std::vector<uint8_t> bytes = getClient(sourceId)->call("obtain", fileName).as<std::vector<uint8_t>>();
	std::ofstream destination(getPath() + fileName, std::ios::binary);
	destination.write((char *)bytes.data(), bytes.size());
	std::cout << "Downloaded " << fileName << " from " << sourceId << std::endl;
	queryCount.lock();
	pendingQueries--;
	queryCount.unlock();
	ready.notify_one();
}

std::vector<uint8_t> obtain(std::string fileName) {
	std::cout << "Obtain request for " << fileName << std::endl;
	//Returns specified file as a vector of bytes
	try {
		std::ifstream file(getPath() + fileName, std::ios::binary);
		file.unsetf(std::ios::skipws);

		std::streampos fileSize;
		file.seekg(0, std::ios::end);
		fileSize = file.tellg();
		file.seekg(0, std::ios::beg);
		std::vector<uint8_t> bytes;
		bytes.reserve(unsigned int(fileSize));
		bytes.insert(bytes.begin(),
			std::istream_iterator<uint8_t>(file),
			std::istream_iterator<uint8_t>());
		return bytes;
	}
	catch (...) {
		rpc::this_handler().respond_error("Error reading file");
		return {};
	}
}

rpc::client* getClient(int clientId) {
	//Return a client for clientId
	//clientsLock.lock();
	const auto leafIter = leafClients.find(clientId);
	if (leafIter != leafClients.end()) {
		//std::cout << "leaf" << std::endl;
		return leafIter->second;
	}
	//If the client doesn't exist yet make a new one
	rpc::client *client = new rpc::client("localhost", 8000 + clientId);
	leafClients.insert({ clientId, client });
	//clientsLock.unlock();
	return client;
}


void start() {
	canStart = true;
	ready.notify_one();
}

void end() {
	canEnd = true;
	ready.notify_one();
}

std::string getPath() {
	return "Leaves/Leaf " + std::to_string(id) + "/";
}