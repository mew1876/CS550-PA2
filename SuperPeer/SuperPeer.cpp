#include "rpc/server.h"
#include "rpc/client.h"
#include <iostream>
#include <vector>
#include <array>
#include <unordered_map>
#include <map>
#include <unordered_set>
#include <algorithm>
#include <mutex>
#include <condition_variable>

void query(int sender, std::array<int, 2> messageId, int TTL, std::string fileName);
void queryHit(int sender, std::array<int, 2> messageId, int TTL, std::string fileName, std::vector<int> leaves);
void add(int leafId, std::string fileName);
void leafReady();
void end();
void dumpIndex();

int id, nSupers, nChildren;
std::vector<int> neighbors;
std::unordered_map<std::string, std::vector<int>> fileIndex;
std::map<std::array<int, 2>, std::unordered_set<int>> messageHistory;

int readyCount = 0;
bool canEnd;
std::mutex countLock;
std::condition_variable ready;

int main(int argc, char* argv[]) {
	//Parse args for ID, nChildren, neighbors
	if (argc < 2) {
		return -1;
	}
	id = std::stoi(argv[0]);
	nSupers = std::stoi(argv[1]);
	nChildren = std::stoi(argv[2]);
	for (int i = 2; i < argc; i++) {
		neighbors.push_back(std::stoi(argv[i]));
	}
	std::cout << "Im a super with ID " << id << std::endl;
	//Start server for file registrations, ready signals, queries, queryhits, and end signal
	rpc::server server(8000 + id);
	server.bind("ready", &leafReady);
	server.bind("add", &add);
	server.bind("query", &query);
	server.bind("queryHit", &queryHit);
	server.bind("end", &end);
	server.async_run(1);
	//Wait for all children to give ready signal
	std::unique_lock<std::mutex> unique(countLock);
	ready.wait(unique, [] { return readyCount >= nChildren; });
	std::cout << "----- Children Ready -----" << std::endl;
	//Send ready signal to system
	rpc::client sysClient("localhost", 8000);
	sysClient.call("ready");
	//Wait for kill signal
	ready.wait(unique, [] { return canEnd; });
}

void query(int sender, std::array<int, 2> messageId, int TTL, std::string fileName) {
	std::cout << "Query from " << sender << " for " << fileName << std::endl;
	//If we've seen the message before, skip query handling
	std::unordered_set<int> &senders = messageHistory[messageId];
	if (senders.empty()) {
		//Check own index for the file
		auto leaves = fileIndex.find(fileName);
		if (leaves != fileIndex.end()) {
			//Reply with queryHit
			std::cout << "File found! Replying with query hit" << std::endl;
			rpc::client replyClient("localhost", 8000 + sender);
			replyClient.call("queryHit", id, messageId, nSupers, fileName, leaves->second);
		}
		else if (TTL - 1 > 0) {
			//Forward query to neighbors
			std::cout << "Forwarding query to neighbors: ";
			for (int neighborId : neighbors) {
				std::cout << " " << neighborId;
				rpc::client forwardClient("localhost", 8000 + neighborId);
				forwardClient.call("query", id, messageId, TTL - 1, fileName);
			}
			std::cout << std::endl;
		}
	}
	//Add new sender to history
	senders.insert(sender);
}

void queryHit(int sender, std::array<int, 2> messageId, int TTL, std::string fileName, std::vector<int> leaves) {
	auto senders = messageHistory.find(messageId);
	if (senders != messageHistory.end() && TTL - 1 > 0) {
		//Forward queryHit to anyone who sent query with messageId
		for (int querySenderId : senders->second) {
			rpc::client forwardClient("localhost", 8000 + querySenderId);
			forwardClient.call("queryHit", id, messageId, TTL - 1, fileName, leaves);
		}
	}
}

void add(int leafId, std::string fileName) {
	std::cout << "File registered: " << leafId << " has " << fileName << std::endl;
	std::vector<int> &leaves = fileIndex[fileName];
	if (std::find(leaves.begin(), leaves.end(), leafId) == leaves.end()) {
		leaves.push_back(leafId);
	}
}

void leafReady() {
	countLock.lock();
	std::cout << "leaf ready" << std::endl;
	readyCount++;
	countLock.unlock();
	ready.notify_one();
}

void end() {
	canEnd = true;
	ready.notify_one();
}

void dumpIndex() {
	std::cout << "Dump:" << std::endl;
	for (auto tuple : fileIndex) {
		std::cout << tuple.first << ": ";
		for (auto IdIter : tuple.second) {
			std::cout << IdIter << " ";
		}
		std::cout << std::endl;
	}
}