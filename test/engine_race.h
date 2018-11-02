// Copyright [2018] Alibaba Cloud All rights reserved
#ifndef ENGINE_RACE_ENGINE_RACE_H_
#define ENGINE_RACE_ENGINE_RACE_H_
#include <string>
#include "include/engine.h"

#include <map>
#include <mutex>

namespace polar_race {


template <typename K, typename V>
class threadsafe_map : public std::map<K, V>
{
public:
    void add(const K& key, const V& val)
    {
        std::lock_guard<std::mutex> lock(mMutex);
        this->insert({key, val});
    }
private:
    mutable std::mutex mMutex;
};


class EngineRace : public Engine  {
 public:
  static RetCode Open(const std::string& name, Engine** eptr);

  explicit EngineRace(const std::string& dir) {
  }

  ~EngineRace();

  RetCode Write(const PolarString& key,
      const PolarString& value) override;

  RetCode Read(const PolarString& key,
      std::string* value) override;

  /*
   * NOTICE: Implement 'Range' in quarter-final,
   *         you can skip it in preliminary.
   */
  RetCode Range(const PolarString& lower,
      const PolarString& upper,
      Visitor &visitor) override;
  
  std::size_t size() const
  {
    return store.size();
  }
 private: 
    threadsafe_map<std::string, std::string> store;
};

}  // namespace polar_race

#endif  // ENGINE_RACE_ENGINE_RACE_H_
