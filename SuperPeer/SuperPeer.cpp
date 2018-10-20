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
rpc::client& getClient(int id);
void leafReady();
void end();
void ping();
void dumpIndex();

int id, nSupers, nChildren;
std::unordered_map<int, rpc::client> neighborClients;
std::unordered_map<int, rpc::client> leafClients;
std::unordered_map<std::string, std::vector<int>> fileIndex;
std::map<std::array<int, 2>, std::unordered_set<int>> messageHistory;

int readyCount = 0;
bool canEnd;
std::mutex countLock;
std::mutex historyLock;
std::mutex indexLock;
std::condition_variable ready;

int main(int argc, char* argv[]) {
	//Parse args for ID, nChildren, neighbors
	if (argc < 2) {
		return -1;
	}
	id = std::stoi(argv[0]);
	nSupers = std::stoi(argv[1]);
	nChildren = std::stoi(argv[2]);
	for (int i = 3; i < argc; i++) {
		neighborClients.emplace(std::piecewise_construct, std::forward_as_tuple(std::stoi(argv[i])), std::forward_as_tuple("localhost", 8000 + std::stoi(argv[i])));
	}
	//Start server for file registrations, pings, ready signals, queries, queryhits, and end signal
	rpc::server server(8000 + id);
	server.bind("ready", &leafReady);
	server.bind("add", &add);
	server.bind("query", &query);
	server.bind("queryHit", &queryHit);
	server.bind("ping", &ping);
	server.bind("end", &end);
	server.async_run(4);
	std::cout << "Im a super with ID " << id << std::endl;
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
	historyLock.lock();
	indexLock.lock();
	std::cout << "Query from " << sender << " for " << fileName << std::endl;
	//If we've seen the message before, skip query handling
	std::unordered_set<int> &senders = messageHistory[messageId];
	if (senders.empty()) {
		//Check own index for the file
		auto leaves = fileIndex.find(fileName);
		if (leaves != fileIndex.end()) {
			//Reply with queryHit
			std::cout << "File found! Replying to " << sender << " about " << fileName << std::endl;
			getClient(sender).async_call("queryHit", id, messageId, nSupers, fileName, leaves->second);
		}
		else if (TTL - 1 > 0) {
			//Forward query to neighbors
			std::cout << "Forwarding query for " << fileName << " to neighbors: ";
			for (auto &neighbor : neighborClients) {
				if (neighbor.first != sender) {
					std::cout << " " << neighbor.first;
					neighbor.second.async_call("query", id, messageId, TTL - 1, fileName);
				}
			}
			std::cout << std::endl;
		}
	}
	//Add new sender to history
	senders.insert(sender);
	indexLock.unlock();
	historyLock.unlock();
}

void queryHit(int sender, std::array<int, 2> messageId, int TTL, std::string fileName, std::vector<int> leaves) {
	historyLock.lock();
	std::cout << "Propagating hit for " << fileName << " to: ";
	auto senders = messageHistory.find(messageId);
	if (senders != messageHistory.end() && TTL - 1 > 0) {
		//Forward queryHit to anyone who sent query with messageId
		for (int querySenderId : senders->second) {
			if (senders->first[0] != sender) {
				std::cout << querySenderId << " ";
				getClient(querySenderId).async_call("queryHit", id, messageId, TTL - 1, fileName, leaves);
			}
		}
	}
	std::cout << "TTL: " << TTL << std::endl;
	historyLock.unlock();
}

void add(int leafId, std::string fileName) {
	indexLock.lock();
	std::cout << "File registered: " << leafId << " has " << fileName << std::endl;
	std::vector<int> &leaves = fileIndex[fileName];
	if (std::find(leaves.begin(), leaves.end(), leafId) == leaves.end()) {
		leaves.push_back(leafId);
	}
	indexLock.unlock();
}

rpc::client& getClient(int clientId) {
	//Return a client - neighbor or leaf
	const auto neighborIter = neighborClients.find(clientId);
	if (neighborIter != neighborClients.end()) {
		return neighborIter->second;
	}
	const auto leafIter = leafClients.find(clientId);
	if (leafIter != leafClients.end()) {
		return leafIter->second;
	}
	//If the client doesn't exist yet, we assume its a leaf
	leafClients.emplace(std::piecewise_construct, std::forward_as_tuple(clientId), std::forward_as_tuple("localhost", 8000 + clientId));
	return leafClients.find(clientId)->second;
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

void ping() {}

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