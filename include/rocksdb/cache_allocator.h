// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <memory>

#include "rocksdb/status.h"

namespace rocksdb {

class Logger;

// CacheAllocator is an interface that a client can implement to supply custom
// cache allocation and deallocation methods. See rocksdb/cache.h for more
// information.
// All methods should be thread-safe.
class CacheAllocator {
 public:
  virtual ~CacheAllocator() = default;

  // Name of the cache allocator, printed in the log
  virtual const char* Name() const = 0;

  // Allocate a block of at least size. Has to be thread-safe.
  virtual void* Allocate(size_t size) = 0;

  // Deallocate previously allocated block. Has to be thread-safe.
  virtual void Deallocate(void* p) = 0;

  // Returns the memory size of the block allocated at p. The default
  // implementation that just returns the original allocation_size is fine.
  virtual size_t UsableSize(void* /*p*/, size_t allocation_size) const {
    // default implementation just returns the allocation size
    return allocation_size;
  }
};

class CacheAllocatorFactory {
 public:
  virtual ~CacheAllocatorFactory() = default;

  virtual const char* Name() const = 0;

  virtual Status NewCacheAllocator(
      std::unique_ptr<CacheAllocator>* cache_allocator) = 0;
};

// Generate cache allocators which allocates through Jemalloc and utilize
// MADV_DONTDUMP through madvice to exclude cache items from core dump.
extern Status NewJemallocNodumpAllocatorFactory(
    std::shared_ptr<CacheAllocatorFactory>* cache_allocator_factory);

}  // namespace rocksdb
