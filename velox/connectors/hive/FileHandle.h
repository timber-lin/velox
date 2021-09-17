/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// A FileHandle is a File pointer plus some (optional, file-type-dependent)
// extra information for speeding up loading columnar data. For example, when
// we open a file we might build a hash map saying what region(s) on disk
// correspond to a given column in a given stripe.
//
// The FileHandle will normally be used in conjunction with a CachedFactory
// to speed up queries that hit the same files repeatedly; see the
// FileHandleCache and FileHandleFactory.

#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include "velox/common/caching/CachedFactory.h"
#include "velox/common/caching/FileIds.h"
#include "velox/common/file/File.h"

namespace facebook::velox {

// See the file comment.
struct FileHandle {
  std::unique_ptr<ReadFile> file;

  // Each time we make a new FileHandle we assign it a uuid and use that id as
  // the identifier in downstream data caching structures. This saves a lot of
  // memory compared to using the filename as the identifier.
  StringIdLease uuid;

  // We'll want to have a hash map here to record the identifier->byte range
  // mappings. Different formats may have different identifiers, so we may need
  // a union of maps. For example in orc you need 3 integers (I think, to be
  // confirmed with xldb): the row bundle, the node, and the sequence. For the
  // first diff we'll not include the map.
};

// Estimates the memory usage of a FileHandle object.
struct FileHandleSizer {
  uint64_t operator()(const FileHandle& a);
};

using FileHandleCache = SimpleLRUCache<std::string, FileHandle>;

// Creates FileHandles via the Generator interface the CachedFactory requires.
class FileHandleGenerator {
 public:
  std::unique_ptr<FileHandle> operator()(const std::string& filename);
};

using FileHandleFactory = CachedFactory<
    std::string,
    FileHandle,
    FileHandleGenerator,
    FileHandleSizer>;

using FileHandleCachedPtr = CachedPtr<std::string, FileHandle>;

} // namespace facebook::velox
