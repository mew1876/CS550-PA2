#include "rpc/server.h"
#include "rpc/client.h"
#include <direct.h>
#include <windows.h>
#include <iostream>
#include <chrono>
#include <vector>
#include <unordered_set>
#include <numeric>
#include <mutex>
#include <condition_variable>

#define ALL_TO_ALL 0
#define LINEAR 1

void superReady();
void leafComplete();
void copyAppend(char *source, char *destination, int destSize, std::string extra);
void run(LPCSTR name, std::string args);

int nSupers = 10, leavesPerSuper = 5, filesPerLeaf = 10, requestsPerLeaf = 10, topology = ALL_TO_ALL, TTL, duplicationFactor = 2;

int readyCount = 0, completeCount = 0;
std::mutex countLock;
std::condition_variable allReady;

int main(int argc, char* argv[]) {
	//Parse args to decide topology, nSupers, leavesPerSuper
	if (argc > 6) {
		nSupers = std::stoi(argv[1]);
		leavesPerSuper = std::stoi(argv[2]);
		filesPerLeaf = std::stoi(argv[3]);
		requestsPerLeaf = std::stoi(argv[4]);
		topology = std::stoi(argv[5]);
		duplicationFactor = std::stoi(argv[6]);
	}
	if (topology == ALL_TO_ALL) {
		TTL = 2;
	}
	else {
		TTL = nSupers;
	}
	//Create server to listen for ready and complete signals
	rpc::server server(8000);
	server.bind("ready", &superReady);
	server.bind("complete", &leafComplete);
	server.async_run(1);
	//Get path to super and leaf executables
	char currentPath[MAX_PATH];
	_getcwd(currentPath, MAX_PATH);
	char superPath[MAX_PATH];
	char leafPath[MAX_PATH];
	copyAppend(currentPath, superPath, MAX_PATH, "\\SuperPeer.exe");
	copyAppend(currentPath, leafPath, MAX_PATH, "\\Leaf.exe");
	//Spawn supers: ID, nSupers, leavesPerSuper, TTL, [neighbors...]
	std::cout << "Spawning Supers" << std::endl;
	int nextId = 1;
	for (int i = 0; i < nSupers; i++) {
		int id = nextId++;
		std::string args = std::to_string(id) + " " + std::to_string(nSupers) + " " + std::to_string(leavesPerSuper) + " " + std::to_string(TTL);
		if (topology == ALL_TO_ALL) {
			for (int neighborId = 1; neighborId <= nSupers; neighborId++) {
				if (neighborId != id) {
					args += " " + std::to_string(neighborId);
				}
			}
		}
		else if (topology == LINEAR) {
			if (id > 1) {
				args += " " + std::to_string(id - 1);
			}
			if (id < nSupers) {
				args += " " + std::to_string(id + 1);
			}
		}
		run(superPath, args);
		//std::cout << "Super args: " << args << std::endl;
	}
	//Spawn leaves: ID, superID, nSupers, TTL, [initial files...], "requests", [requests...]
	std::vector<int> numbers(nSupers * leavesPerSuper * filesPerLeaf / duplicationFactor);
	std::unordered_set<int> used;
	std::iota(numbers.begin(), numbers.end(), 1);
	std::cout << "Spawning Leaves" << std::endl;
	std::srand(unsigned int(std::time(nullptr)));
	std::vector<std::unordered_set<int>> initialFiles;
	//Choose random initial files
	for (int i = 0; i < nSupers * leavesPerSuper; i++) {
		initialFiles.push_back({});
		std::unordered_set<int> &files = initialFiles[i];
		std::random_shuffle(numbers.begin(), numbers.end());
		for (int j = 0; j < filesPerLeaf; j++) {
			files.insert(numbers[j]);
			used.insert(numbers[j]);
		}		
	}	
	std::vector<int> usedVector(used.begin(), used.end());
	int totalRequests = 0;
	for (int i = 0; i < nSupers * leavesPerSuper; i++) {
		//Choose random requests
		std::unordered_set<int> requestFiles;
		int numRequests = 0;
		for (int usedNum : used) {
			if (initialFiles[i].find(usedNum) == initialFiles[i].end()) {
				numRequests++;
			}
		}
		numRequests = std::min(requestsPerLeaf, numRequests);
		totalRequests += numRequests;
		for (int j = 0; j < numRequests; j++) {
			int requestNum;
			do {
				requestNum = usedVector[std::rand() % usedVector.size()];
			} while (requestFiles.find(requestNum) != requestFiles.end() || initialFiles[i].find(requestNum) != initialFiles[i].end());
			requestFiles.insert(requestNum);
		}
		//Build args and spawn leaf
		std::string args = std::to_string(nextId++) + " " + std::to_string(i % nSupers + 1) + " " + std::to_string(nSupers) + " " + std::to_string(TTL);
		for (auto initial : initialFiles[i]) {
				args += " " + std::to_string(initial) + ".txt";
		}
		args += " requests";
		for (auto request : requestFiles) {
			args += " " + std::to_string(request) + ".txt";
		}
		run(leafPath, args);
		//std::cout << "Leaf args: " << args << std::endl;
	}
	//Wait for all supers to give ready signal
	std::unique_lock<std::mutex> unique(countLock);
	allReady.wait(unique, [] { return readyCount >= nSupers; });
	std::cout << "Supers are ready" << std::endl;
	//Start timer
	auto startTime = std::chrono::high_resolution_clock::now();
	//Tell leaves to begin making requests
	std::cout << "Starting Leaf requests" << std::endl;
	for (int i = nSupers + 1; i < nextId; i++) {
		rpc::client sysClient("localhost", 8000 + i);
		sysClient.call("start");
	}
	//Wait for all leaves to give complete signal
	allReady.wait(unique, [] { return completeCount >= nSupers * leavesPerSuper; });
	std::cout << "Leaves have finished" << std::endl;
	//End timer
	std::chrono::duration<double> duration = std::chrono::high_resolution_clock::now() - startTime;
	std::cout << totalRequests << " requests took " << duration.count() << " seconds. R/s = " << std::to_string(totalRequests / duration.count()) << std::endl;
	//Send end signal to all supers and leaves
	std::vector<rpc::client*> clients;
	for (int i = 1; i < nextId; i++) {
		rpc::client *client = new rpc::client("localhost", 8000 + i);
		clients.push_back(client);
		client->async_call("end");
	}
	std::cout << "Press Enter to exit" << std::endl;
	std::cin.get();
	for (auto client : clients) {
		delete client;
	}
}

void superReady() {
	countLock.lock();
	readyCount++;
	countLock.unlock();
	allReady.notify_one();
}

void leafComplete() {
	countLock.lock();
	completeCount++;
	countLock.unlock();
	allReady.notify_one();
}

void copyAppend(char *source, char *destination, int destSize, std::string extra) {
	strcpy_s(destination, destSize, source);
	strcat_s(destination, destSize, extra.c_str());
}

void run(LPCSTR name, std::string args) {
	//execution code from stackoverflow answer
	//https://stackoverflow.com/questions/15435994/how-do-i-open-an-exe-from-another-c-exe
	STARTUPINFO si;
	PROCESS_INFORMATION pi;
	
	const int MAX_ARG = 512;
	char cArg[MAX_ARG];
	strcpy_s(cArg, MAX_ARG, args.c_str());

	// set the size of the structures
	ZeroMemory(&si, sizeof(si));
	si.cb = sizeof(si);
	ZeroMemory(&pi, sizeof(pi));

	int success = CreateProcess(name,   // the path
		cArg,			// Command line
		NULL,           // Process handle not inheritable
		NULL,           // Thread handle not inheritable
		FALSE,          // Set handle inheritance to FALSE
		CREATE_NEW_CONSOLE,              // No creation flags
		NULL,           // Use parent's environment block
		NULL,           // Use parent's starting directory 
		&si,            // Pointer to STARTUPINFO structure
		&pi             // Pointer to PROCESS_INFORMATION structure (removed extra parentheses)
	);

	// Close process and thread handles. 
	CloseHandle(pi.hProcess);
	CloseHandle(pi.hThread);
}