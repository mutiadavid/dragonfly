// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "core/btree_set.h"

#include <random>

#include "base/gtest.h"
#include "base/logging.h"
#include "core/mi_memory_resource.h"

using namespace std;

namespace dfly {

class BTreeSetTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
  }
};

TEST_F(BTreeSetTest, BtreeInsert) {
  MiMemoryResource mi_alloc(mi_heap_get_backing());
  BTree btree(&mi_alloc);

  mt19937 generator(1);

  for (unsigned i = 1; i < 7000; ++i) {
    btree.Insert(i);
  }
  btree.Validate();
  ASSERT_GT(mi_alloc.used(), 56000u);
  ASSERT_LT(mi_alloc.used(), 66000u);

  for (unsigned i = 1; i < 7000; ++i) {
    ASSERT_TRUE(btree.Contains(i));
  }

  btree.Clear();
  ASSERT_EQ(mi_alloc.used(), 0u);

  uniform_int_distribution<uint64_t> dist(0, 100000);
  for (unsigned i = 0; i < 20000; ++i) {
    btree.Insert(dist(generator));
  }
  LOG(INFO) << btree.Height() << " " << btree.Size();
  btree.Validate();
  ASSERT_GT(mi_alloc.used(), 10000u);
  btree.Clear();
  ASSERT_EQ(mi_alloc.used(), 0u);

  for (unsigned i = 20000; i > 1; --i) {
    btree.Insert(i);
  }
  btree.Validate();
  LOG(INFO) << btree.Height() << " " << btree.Size();
  ASSERT_GT(mi_alloc.used(), 20000 * 8);
  ASSERT_LT(mi_alloc.used(), 20000 * 10);
  btree.Clear();
  ASSERT_EQ(mi_alloc.used(), 0u);
}

TEST_F(BTreeSetTest, BtreeDelete) {
  MiMemoryResource mi_alloc(mi_heap_get_backing());
  BTree btree(&mi_alloc);

  for (unsigned i = 31; i > 10; --i) {
    btree.Insert(i);
  }

  for (unsigned i = 1; i < 10; ++i) {
    ASSERT_FALSE(btree.Delete(i));
  }

  for (unsigned i = 11; i < 32; ++i) {
    ASSERT_TRUE(btree.Delete(i));
  }
  ASSERT_EQ(mi_alloc.used(), 0u);
  ASSERT_EQ(btree.Size(), 0u);

  constexpr size_t kNumElems = 7000;
  for (unsigned i = 0; i < kNumElems; ++i) {
    btree.Insert(i);
  }

  unsigned sz = btree.Size();
  for (unsigned i = 0; i < kNumElems; ++i) {
    ASSERT_TRUE(btree.Delete(i));
    ASSERT_EQ(btree.Size(), --sz);
  }

  ASSERT_EQ(mi_alloc.used(), 0u);
  ASSERT_EQ(btree.Size(), 0u);
}

}  // namespace dfly
