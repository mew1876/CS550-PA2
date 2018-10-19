#include "rpc/server.h"
#include "rpc/client.h"
#include "rpc/this_handler.h"
#include <iostream>
#include <fstream>
#include <array>
#include <mutex>
#include <condition_variable>

void queryHit(int sender, std::array<int, 2> messageId, int TTL, std::string fileName, std::vector<int> leaves);
std::vector<uint8_t> obtain(std::string fileName);
void start();
void end();
std::string getPath();

int id, superId, nSupers;
int nextMessageId = 0;
int pendingQueries = 0;

bool canStart = false, canEnd = false;
std::mutex waitLock;
std::mutex queryCount;
std::condition_variable ready;

int main(int argc, char* argv[]) {
	//Parse args for ID, files to start with, files to request
	if (argc < 3) {
		return -1;
	}
	id = std::stoi(argv[0]);
	superId = std::stoi(argv[1]);
	nSupers = std::stoi(argv[2]);
	std::cout << "Im a leaf with ID " << id << " and my super's ID is " << superId << std::endl;
	//Start server for start, obtain, and end signals
	rpc::server server(8000 + id);
	server.bind("start", &start);
	server.bind("queryHit", &queryHit);
	server.bind("obtain", &obtain);
	server.bind("end", &end);
	server.async_run(1);
	//Create super client
	rpc::client superClient("localhost", 8000 + superId);
	//Create init files & add to super index
	CreateDirectory("Leaves", NULL);
	CreateDirectory(getPath().c_str(), NULL);
	int argIndex;
	for (argIndex = 3; argIndex < argc; argIndex++) {
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
		superClient.call("add", id, fileName);
	}
	//Send ready signal to super
	superClient.call("ready");
	//Wait for start signal
	std::unique_lock<std::mutex> unique(waitLock);
	ready.wait(unique, [] { return canStart; });
	//Make file requests
	for (; argIndex < argc; argIndex++) {
		std::string fileName(argv[argIndex]);
		std::cout << "Querying for " << fileName << std::endl;
		std::array<int, 2> messageId = { id, nextMessageId++ };
		superClient.call("query", id, messageId, nSupers, fileName);
		queryCount.lock();
		pendingQueries++;
		queryCount.unlock();
	}
	ready.wait(unique, [] { return pendingQueries == 0; });
	//Send complete signal to system
	rpc::client sysClient("localhost", 8000);
	sysClient.call("complete");
	//Wait for kill signal
	ready.wait(unique, [] { return canEnd; });
}

void queryHit(int sender, std::array<int, 2> messageId, int TTL, std::string fileName, std::vector<int> leaves) {
	std::cout << "Query hit receieved!" << std::endl;
	std::cout << fileName << " is at: ";
	for (int leaf : leaves) {
		std::cout << leaf << " ";
	}
	std::cout << std::endl;
	queryCount.lock();
	pendingQueries--;
	queryCount.unlock();
}

std::vector<uint8_t> obtain(std::string fileName) {
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