#include <iostream>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <cassert>

#include <pthread.h>
#include <atomic>
#include <mutex>
#include <thread>
#include <random>

#include <vector>
#include <algorithm>

#include "include/engine.h"

using namespace polar_race;

template <typename T>
class threadsafe_vector : public std::vector<T>
{
public:
    void add(const T& val)
    {
        std::lock_guard<std::mutex> lock(mMutex);
        this->push_back(val);
    }

    void add(T&& val)
    {
        std::lock_guard<std::mutex> lock(mMutex);
        this->emplace_back(val);
    }

private:
    mutable std::mutex mMutex;
};

class RandNum_generator
{
private:
    RandNum_generator(const RandNum_generator&) = delete;
    RandNum_generator& operator=(const RandNum_generator&) = delete;
    std::uniform_int_distribution<unsigned> u;
    std::default_random_engine e;
    int mStart, mEnd;
public:
    // [start, end], inclusive, uniformally distributed
    RandNum_generator(int start, int end)
        : u(start, end)
        , e(std::hash<std::thread::id>()(std::this_thread::get_id())
            + std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now().time_since_epoch()).count())
        , mStart(start), mEnd(end)
    {}

    // [mStart, mEnd], inclusive
    unsigned nextNum()
    {
        return u(e);
    }

    // [0, max], inclusive
    unsigned nextNum(unsigned max)
    {
        return unsigned((u(e) - mStart) / float(mEnd - mStart) * max);
    }
};

// randomly-sampled positions with the following Python code
// "np.random.choice(np.arange(4096), 8, replace=False)"
//
// make sure that random_str and key_from_value use the same index array
unsigned kRandomIdxes[]{0, 1, 2, 3, 4, 5, 6, 7};
std::string random_str(RandNum_generator& rng, std::size_t strLen)
{
    std::string rs(strLen, ' ');
    for (auto idx : kRandomIdxes) {
        rs[idx] = rng.nextNum();
    }
    return rs;
}
std::string key_from_value(const std::string& val)
{
    std::string key(8, ' ');
    for (unsigned i = 0; i < 8; ++i) {
        key[i] = val[kRandomIdxes[i]];
    }
    return key;
}

void Write(Engine* engine, threadsafe_vector<std::string>& keys, unsigned numWrite)
{
    RandNum_generator rng(0, 255);
    for (unsigned i = 0; i < numWrite; ++i) {
        std::string val(random_str(rng, 4096));
        std::string key(key_from_value(val));
        engine->Write(key, val);
        keys.add(key);
    }
}

void RandomRead(Engine* engine, const threadsafe_vector<std::string>& keys, unsigned numRead)
{
    RandNum_generator rng(0, keys.size() - 1);
    for (unsigned i = 0; i < numRead; ++i) {
        auto& key = keys[rng.nextNum()];
        std::string val;
        engine->Read(key, &val);
        if (key != key_from_value(val)) {
            std::cout << "Random Read error: key and value not match" << std::endl;
            exit(-1);
        }
    }
}

class MyVisitor : public Visitor
{
public:
    MyVisitor(const threadsafe_vector<std::string>& keys, unsigned start, unsigned& cnt)
        : mKeys(keys), mStart(start), mCnt(cnt)
    {}

    ~MyVisitor() {}

    void Visit(const PolarString& key, const PolarString& value)
    {
        if (key != key_from_value(value.ToString())) {
            std::cout << "Sequential Read error: key and value not match" << std::endl;
            exit(-1);
        }
        if (key != mKeys[mStart + mCnt]) {
            std::cout << "Sequential Read error: not an expected key" << std::endl;
            exit(-1);
        }
        mCnt += 1;
    }

private:
    const threadsafe_vector<std::string>& mKeys;
    unsigned mStart;
    unsigned& mCnt;
};

void sequentialRead(Engine* engine, const threadsafe_vector<std::string>& keys)
{
    RandNum_generator rng(0, keys.size() - 1);
    RandNum_generator rng1(10, 100);

    unsigned lenKeys = keys.size();
    // Random ranges
    unsigned lenAccu = 0;
    while (lenAccu < lenKeys) {
        std::string lower, upper;

        unsigned start = rng.nextNum();
        lower = keys[start];

        unsigned len = rng1.nextNum();
        if (start + len >= lenKeys) {
            len = lenKeys - start;
        }
        if (start + len == lenKeys) {
            upper = "";
        } else {
            upper = keys[start + len];
        }

        unsigned keyCnt = 0;
        MyVisitor visitor(keys, start, keyCnt);
        engine->Range(lower, upper, visitor);
        if (keyCnt != len) {
            std::cout << "Range size not match, expected: " << len
                      << " actual: " << keyCnt << std::endl;
            exit(-1);
        }

        lenAccu += len;
    }

    // Whole range traversal
    unsigned keyCnt = 0;
    MyVisitor visitor(keys, 0, keyCnt);
    engine->Range("", "", visitor);
    if (keyCnt != lenKeys) {
        std::cout << "Range size not match, expected: " << lenKeys
                  << " actual: " << keyCnt << std::endl;
        exit(-1);
    }
}

#include <dirent.h>
void removeDir(const std::string& dirPath)
{
    DIR* dir = opendir(dirPath.c_str());
    struct dirent *entry;
    while ((entry = readdir(dir)) != NULL) {
        //std::cout << entry->d_name << " " << entry->d_type << std::endl;
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
            continue;
        std::string filePath(dirPath + "/" + entry->d_name);
        struct stat fStat;
        lstat(filePath.c_str(), &fStat);
        if (S_ISDIR(fStat.st_mode)) {
            removeDir(filePath);
        } else if (S_ISREG(fStat.st_mode)){
            std::cout << "Removing file: " << filePath << std::endl;
            unlink(filePath.c_str());
        }
    }
    closedir(dir);
    std::cout << "Removing dir:  " << dirPath << std::endl;
    rmdir(dirPath.c_str());
}

void test(const std::string&, unsigned, unsigned);
void test_with_kill(const std::string&, unsigned, unsigned);

int main(int argc, char** argv)
{
    if(argc < 2){
        std::cout << "Too less ARGS!!" << std::endl;
        abort();
    }
    std::string dir(argv[1]);

    auto numThreads = 4;
    unsigned numWrite = 1000;
    std::cout << numThreads << " " << numWrite << std::endl;

    bool withKill = false;
    if (withKill)
        test_with_kill(dir, numThreads, numWrite);
    else
        test(dir, numThreads, numWrite);

    removeDir(dir);

    return 0;
}

void test(const std::string& dir, unsigned numThreads, unsigned numWrite)
{
    Engine *engine = NULL;
    RetCode ret = Engine::Open(dir.c_str(), &engine);
    assert(ret == kSucc);

    threadsafe_vector<std::string> keys;
    keys.reserve(numThreads * numWrite);

    // Write
    auto writeStart = std::chrono::high_resolution_clock::now();

    std::vector<std::thread> writers;
    for (int i = 0; i < numThreads; ++i) {
        writers.emplace_back(std::thread(Write, engine, std::ref(keys), numWrite));
    }
    for (auto& th : writers) {
        th.join();
    }
    writers.clear();

    auto writeEnd = std::chrono::high_resolution_clock::now();
    std::cout << "Writing takes: "
              << std::chrono::duration<double, std::milli>(writeEnd - writeStart).count()
              << " milliseconds" << std::endl;


    // Random Read
    auto rreadStart = std::chrono::high_resolution_clock::now();

    std::vector<std::thread> rreaders;
    for (int i = 0; i < numThreads; ++i) {
        rreaders.emplace_back(std::thread(RandomRead, engine, std::cref(keys), numWrite));
    }
    for (auto& th : rreaders) {
        th.join();
    }
    rreaders.clear();

    auto rreadEnd = std::chrono::high_resolution_clock::now();
    std::cout << "Random read takes: "
              << std::chrono::duration<double, std::milli>(rreadEnd - rreadStart).count()
              << " milliseconds" << std::endl;


    // Sequential Read
    std::cout << "Total: " << keys.size() << std::endl;
    std::sort(keys.begin(), keys.end());
    auto last = std::unique(keys.begin(), keys.end());
    keys.erase(last, keys.end());
    std::cout << "Unique: " << keys.size() << std::endl;

    auto sreadStart = std::chrono::high_resolution_clock::now();

    std::vector<std::thread> sreaders;
    for (int i = 0; i < numThreads; ++i) {
        sreaders.emplace_back(std::thread(sequentialRead, engine, std::cref(keys)));
    }
    for (auto& th : sreaders) {
        th.join();
    }
    sreaders.clear();

    auto sreadEnd = std::chrono::high_resolution_clock::now();
    std::cout << "Sequential read takes: "
              << std::chrono::duration<double, std::milli>(sreadEnd - sreadStart).count()
              << " milliseconds" << std::endl;

    delete engine;
}

void test_with_kill(const std::string& dir, unsigned numThreads, unsigned numWrite)
{
    Engine *engine = NULL;

    unsigned numKills = 4;
    threadsafe_vector<std::string> keys;
    keys.reserve(numThreads * numWrite);

    // Write
    double duration = 0;
    for (int nk = 0; nk < numKills; ++nk) {
        RetCode ret = Engine::Open(dir.c_str(), &engine);
        assert(ret == kSucc);

        auto writeStart = std::chrono::high_resolution_clock::now();

        std::vector<std::thread> writers;
        for (int i = 0; i < numThreads; ++i) {
            writers.emplace_back(std::thread(Write, engine, std::ref(keys), numWrite / numKills));
        }
        for (auto& th : writers) {
            th.join();
        }
        writers.clear();

        auto writeEnd = std::chrono::high_resolution_clock::now();
        duration += std::chrono::duration<double, std::milli>(writeEnd - writeStart).count();

        delete engine;
    }

    std::cout << "Writing takes: "
              << duration
              << " milliseconds" << std::endl;

    RetCode ret = Engine::Open(dir.c_str(), &engine);
    assert (ret == kSucc);

    // Random Read
    auto rreadStart = std::chrono::high_resolution_clock::now();

    std::vector<std::thread> rreaders;
    for (int i = 0; i < numThreads; ++i) {
        rreaders.emplace_back(std::thread(RandomRead, engine, std::cref(keys), numWrite));
    }
    for (auto& th : rreaders) {
        th.join();
    }
    rreaders.clear();

    auto rreadEnd = std::chrono::high_resolution_clock::now();
    std::cout << "Random read takes: "
              << std::chrono::duration<double, std::milli>(rreadEnd - rreadStart).count()
              << " milliseconds" << std::endl;


    // Sequential Read
    std::cout << "Total: " << keys.size() << std::endl;
    std::sort(keys.begin(), keys.end());
    auto last = std::unique(keys.begin(), keys.end());
    keys.erase(last, keys.end());
    std::cout << "Unique: " << keys.size() << std::endl;

    auto sreadStart = std::chrono::high_resolution_clock::now();

    std::vector<std::thread> sreaders;
    for (int i = 0; i < numThreads; ++i) {
        sreaders.emplace_back(std::thread(sequentialRead, engine, std::cref(keys)));
    }
    for (auto& th : sreaders) {
        th.join();
    }
    sreaders.clear();

    auto sreadEnd = std::chrono::high_resolution_clock::now();
    std::cout << "Sequential read takes: "
              << std::chrono::duration<double, std::milli>(sreadEnd - sreadStart).count()
              << " milliseconds" << std::endl;

    delete engine;
}