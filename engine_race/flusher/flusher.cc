//
// Created by qzwlecr on 10/31/18.
//

#include <flusher/flusher.h>

namespace polar_race {
	using namespace std;
	char CommitQueue[COMMIT_QUEUE_LENGTH * 4096];
	bool CommitCompletionQueue[COMMIT_QUEUE_LENGTH];
	uint64_t WrittenIndex;
	char InternalBuffer[INTERNAL_BUFFER_LENGTH];

	void Flusher::flush_begin() {
		thread flush_reader(&Flusher::read, this);
		flush_reader.detach();
		thread flush_executor(&Flusher::flush, this);
		flush_executor.detach();
		return;
	}

	void *Flusher::read() {
		for (uint64_t index = 0; ; index ++) {
			if (index == COMMIT_QUEUE_LENGTH)
				index = 0;
			while (CommitCompletionQueue[index] == 0)
				if (UNLIKELY(ExitSign) == true) {
					last_flush = true;
					return nullptr;
				}
			while (internal_buffer_index == INTERNAL_BUFFER_LENGTH);
			InternalBuffer[internal_buffer_index] = CommitQueue[index];

			CommitCompletionQueue[index] = 0;
		}

	}

	void *Flusher::flush() {
		int fd = open(VALUES_PATH.c_str(), O_APPEND | O_SYNC | O_CREAT | O_DIRECT);
		while (1) {
			if (internal_buffer_index == INTERNAL_BUFFER_LENGTH) {
				write(fd, InternalBuffer, INTERNAL_BUFFER_LENGTH);
				WrittenIndex += INTERNAL_BUFFER_LENGTH;
				internal_buffer_index = 0;
			} else if (UNLIKELY(ExitSign) == true) {
				while (!last_flush);
				write(fd, InternalBuffer, internal_buffer_index);
				WrittenIndex += internal_buffer_index;
				internal_buffer_index = 0;
				//TODO: Serialize hash table before exit.
				exit(0);
			}
		}
	}
}
