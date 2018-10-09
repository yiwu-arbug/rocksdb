//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include "cache/sharded_cache.h"

#include <string>

#include "util/mutexlock.h"

namespace rocksdb {

ShardedCache::ShardedCache(
    size_t capacity, int num_shard_bits, bool strict_capacity_limit,
    std::shared_ptr<CacheAllocatorFactory> cache_allocator_factory,
    bool shard_cache_allocator, std::unique_ptr<CacheAllocator> cache_allocator,
    std::unique_ptr<CacheAllocator>* cache_allocator_shards)
    : num_shard_bits_(num_shard_bits),
      cache_allocator_factory_(std::move(cache_allocator_factory)),
      shard_cache_allocator_(shard_cache_allocator),
      capacity_(capacity),
      strict_capacity_limit_(strict_capacity_limit),
      last_id_(1),
      cache_allocator_(std::move(cache_allocator)),
      cache_allocator_shards_(cache_allocator_shards) {
  if (cache_allocator_factory_ != nullptr && shard_cache_allocator) {
    assert(cache_allocator_shards_ != nullptr);
  }
}

ShardedCache::~ShardedCache() {
  if (cache_allocator_shards_ != nullptr) {
    assert(cache_allocator_factory_ != nullptr && shard_cache_allocator_);
    delete[] cache_allocator_shards_;
  }
}

void ShardedCache::SetCapacity(size_t capacity) {
  int num_shards = 1 << num_shard_bits_;
  const size_t per_shard = (capacity + (num_shards - 1)) / num_shards;
  MutexLock l(&capacity_mutex_);
  for (int s = 0; s < num_shards; s++) {
    GetShard(s)->SetCapacity(per_shard);
  }
  capacity_ = capacity;
}

void ShardedCache::SetStrictCapacityLimit(bool strict_capacity_limit) {
  int num_shards = 1 << num_shard_bits_;
  MutexLock l(&capacity_mutex_);
  for (int s = 0; s < num_shards; s++) {
    GetShard(s)->SetStrictCapacityLimit(strict_capacity_limit);
  }
  strict_capacity_limit_ = strict_capacity_limit;
}

Status ShardedCache::Insert(const Slice& key, void* value, size_t charge,
                            void (*deleter)(const Slice& key, void* value),
                            Handle** handle, Priority priority) {
  uint32_t hash = HashSlice(key);
  return GetShard(Shard(hash))
      ->Insert(key, hash, value, charge, deleter, handle, priority);
}

Cache::Handle* ShardedCache::Lookup(const Slice& key, Statistics* /*stats*/) {
  uint32_t hash = HashSlice(key);
  return GetShard(Shard(hash))->Lookup(key, hash);
}

bool ShardedCache::Ref(Handle* handle) {
  uint32_t hash = GetHash(handle);
  return GetShard(Shard(hash))->Ref(handle);
}

bool ShardedCache::Release(Handle* handle, bool force_erase) {
  uint32_t hash = GetHash(handle);
  return GetShard(Shard(hash))->Release(handle, force_erase);
}

void ShardedCache::Erase(const Slice& key) {
  uint32_t hash = HashSlice(key);
  GetShard(Shard(hash))->Erase(key, hash);
}

uint64_t ShardedCache::NewId() {
  return last_id_.fetch_add(1, std::memory_order_relaxed);
}

size_t ShardedCache::GetCapacity() const {
  MutexLock l(&capacity_mutex_);
  return capacity_;
}

bool ShardedCache::HasStrictCapacityLimit() const {
  MutexLock l(&capacity_mutex_);
  return strict_capacity_limit_;
}

size_t ShardedCache::GetUsage() const {
  // We will not lock the cache when getting the usage from shards.
  int num_shards = 1 << num_shard_bits_;
  size_t usage = 0;
  for (int s = 0; s < num_shards; s++) {
    usage += GetShard(s)->GetUsage();
  }
  return usage;
}

size_t ShardedCache::GetUsage(Handle* handle) const {
  return GetCharge(handle);
}

size_t ShardedCache::GetPinnedUsage() const {
  // We will not lock the cache when getting the usage from shards.
  int num_shards = 1 << num_shard_bits_;
  size_t usage = 0;
  for (int s = 0; s < num_shards; s++) {
    usage += GetShard(s)->GetPinnedUsage();
  }
  return usage;
}

void ShardedCache::ApplyToAllCacheEntries(void (*callback)(void*, size_t),
                                          bool thread_safe) {
  int num_shards = 1 << num_shard_bits_;
  for (int s = 0; s < num_shards; s++) {
    GetShard(s)->ApplyToAllCacheEntries(callback, thread_safe);
  }
}

void ShardedCache::EraseUnRefEntries() {
  int num_shards = 1 << num_shard_bits_;
  for (int s = 0; s < num_shards; s++) {
    GetShard(s)->EraseUnRefEntries();
  }
}

std::string ShardedCache::GetPrintableOptions() const {
  std::string ret;
  ret.reserve(20000);
  const int kBufferSize = 200;
  char buffer[kBufferSize];
  {
    MutexLock l(&capacity_mutex_);
    snprintf(buffer, kBufferSize, "    capacity : %" ROCKSDB_PRIszt "\n",
             capacity_);
    ret.append(buffer);
    snprintf(buffer, kBufferSize, "    num_shard_bits : %d\n", num_shard_bits_);
    ret.append(buffer);
    snprintf(buffer, kBufferSize, "    strict_capacity_limit : %d\n",
             strict_capacity_limit_);
    ret.append(buffer);
  }
  snprintf(
      buffer, kBufferSize, "    cache_allocator_factory : %s\n",
      cache_allocator_factory_ ? cache_allocator_factory_->Name() : "None");
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "    shard_cache_allocator : %d\n",
           shard_cache_allocator_);
  ret.append(buffer);
  ret.append(GetShard(0)->GetPrintableOptions());
  return ret;
}

Status ShardedCache::InitCacheAllocator(
    CacheAllocatorFactory* cache_allocator_factory, bool shard_cache_allocator,
    int num_shards, std::unique_ptr<CacheAllocator>* cache_allocator,
    std::unique_ptr<CacheAllocator>** cache_allocator_shards) {
  assert(cache_allocator != nullptr && *cache_allocator == nullptr);
  assert(cache_allocator_shards != nullptr &&
         *cache_allocator_shards == nullptr);
  if (cache_allocator_factory == nullptr) {
    return Status::OK();
  }
  Status s;
  if (!shard_cache_allocator) {
    s = cache_allocator_factory->NewCacheAllocator(cache_allocator);
  } else {
    *cache_allocator_shards =
        new std::unique_ptr<CacheAllocator>[num_shards];
    for (int i = 0; i < num_shards; i++) {
      s = cache_allocator_factory->NewCacheAllocator(
          &(*cache_allocator_shards)[i]);
      if (!s.ok()) {
        break;
      }
    }
  }
  return s;
}

CacheAllocator* ShardedCache::GetCacheAllocator(const Slice& key) {
  if (cache_allocator_factory_ == nullptr) {
    return nullptr;
  }
  if (!shard_cache_allocator_) {
    return cache_allocator_.get();
  } else {
    assert(cache_allocator_shards_ != nullptr);
    uint32_t hash = HashSlice(key);
    return cache_allocator_shards_[Shard(hash)].get();
  }
}

int GetDefaultCacheShardBits(size_t capacity) {
  int num_shard_bits = 0;
  size_t min_shard_size = 512L * 1024L;  // Every shard is at least 512KB.
  size_t num_shards = capacity / min_shard_size;
  while (num_shards >>= 1) {
    if (++num_shard_bits >= 6) {
      // No more than 6.
      return num_shard_bits;
    }
  }
  return num_shard_bits;
}

}  // namespace rocksdb
