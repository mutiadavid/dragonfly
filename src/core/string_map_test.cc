// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/string_map.h"

#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>
#include <gtest/gtest.h>
#include <mimalloc.h>

#include <algorithm>
#include <cstddef>
#include <memory_resource>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

#include "core/compact_object.h"
#include "core/mi_memory_resource.h"
#include "glog/logging.h"
#include "redis/sds.h"

extern "C" {
#include "redis/zmalloc.h"
}

namespace dfly {

using namespace std;
using absl::StrCat;

class StringMapTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    auto* tlh = mi_heap_get_backing();
    init_zmalloc_threadlocal(tlh);
  }

  static void TearDownTestSuite() {
    mi_heap_collect(mi_heap_get_backing(), true);

    auto cb_visit = [](const mi_heap_t* heap, const mi_heap_area_t* area, void* block,
                       size_t block_size, void* arg) {
      LOG(ERROR) << "Unfreed allocations: block_size " << block_size
                 << ", allocated: " << area->used * block_size;
      return true;
    };

    mi_heap_visit_blocks(mi_heap_get_backing(), false /* do not visit all blocks*/, cb_visit,
                         nullptr);
  }

  StringMapTest() : mi_alloc_(mi_heap_get_backing()) {
  }

  void SetUp() override {
    sm_.reset(new StringMap(&mi_alloc_));
  }

  void TearDown() override {
    sm_.reset();
    EXPECT_EQ(zmalloc_used_memory_tl, 0);
  }

  MiMemoryResource mi_alloc_;
  std::unique_ptr<StringMap> sm_;
};

TEST_F(StringMapTest, Basic) {
  EXPECT_TRUE(sm_->AddOrUpdate("foo", "bar"));
  EXPECT_TRUE(sm_->Contains("foo"));
  EXPECT_STREQ("bar", sm_->Find("foo"));

  auto it = sm_->begin();
  EXPECT_STREQ("foo", it->first);
  EXPECT_STREQ("bar", it->second);
  ++it;
  EXPECT_TRUE(it == sm_->end());

  for (const auto& k_v : *sm_) {
    EXPECT_STREQ("foo", k_v.first);
    EXPECT_STREQ("bar", k_v.second);
  }

  size_t sz = sm_->ObjMallocUsed();
  EXPECT_FALSE(sm_->AddOrUpdate("foo", "baraaaaaaaaaaaa2"));
  EXPECT_GT(sm_->ObjMallocUsed(), sz);
  it = sm_->begin();
  EXPECT_STREQ("baraaaaaaaaaaaa2", it->second);

  EXPECT_FALSE(sm_->AddOrSkip("foo", "bar2"));
  EXPECT_STREQ("baraaaaaaaaaaaa2", it->second);
}

TEST_F(StringMapTest, EmptyFind) {
  sm_->Find("bar");
}

TEST_F(StringMapTest, Ttl) {
  EXPECT_TRUE(sm_->AddOrUpdate("bla", "val1", 1));
  EXPECT_FALSE(sm_->AddOrUpdate("bla", "val2", 1));
  sm_->set_time(1);
  EXPECT_TRUE(sm_->AddOrUpdate("bla", "val2", 1));
  EXPECT_EQ(1u, sm_->Size());

  EXPECT_FALSE(sm_->AddOrSkip("bla", "val3", 2));

  // set ttl to 2, meaning that the key will expire at time 3.
  EXPECT_TRUE(sm_->AddOrSkip("bla2", "val3", 2));
  EXPECT_TRUE(sm_->Contains("bla2"));

  sm_->set_time(3);
  auto it = sm_->begin();
  EXPECT_TRUE(it == sm_->end());
}

}  // namespace dfly
