#include "hashmap.h"
#include <iostream>
#include <thread>

AtomicUnorderedInsertMap<uint64_t, MutableAtom<uint64_t>> m(64000000);

void insert(uint64_t start) {
	for (uint64_t key = start; key < start + 1000000; key++) {
		m.findOrConstruct(key, [&](void *raw) {
			new(raw) uint64_t(std::forward(key));
		});
	}
}

int main() {
	std::thread thrs[64];
	auto start = std::chrono::high_resolution_clock::now();

	for (int i = 0; i < 64; i++) {
		thrs[i] = std::thread(insert, i * 1000000);
	}
	for (int i = 0; i < 64; i++) {
		thrs[i].join();

	}
	auto end = std::chrono::high_resolution_clock::now();
	long long microseconds = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
	std::cout << microseconds << std::endl;
	start = std::chrono::high_resolution_clock::now();
	for (uint64_t key = 0; key < 64000000; key++) {
		if ((*m.find(key)).second != key) {
			std::cout << "wtf" << std::endl;
			return 0;
		}
	}
	end = std::chrono::high_resolution_clock::now();
	microseconds = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
	std::cout << microseconds << std::endl;
}
