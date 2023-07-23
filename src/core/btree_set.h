// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/base/internal/endian.h>

#include <cstdint>

#include "base/pmr/memory_resource.h"

namespace dfly {

class BTree;

namespace detail {

/*
 Each node has degree of freedom to contain keys between [min_elems, max_elems] inclusive.
 min_elems = max_elems / 2.
  Once a node is full, it must be split into two nodes. The split is done by moving the median
  key to the parent node and creating a new node with the rest of the keys. The median key
  is the key that is moved to the parent node. The median key is the last key in the left node.
  All keys in the right node are greater than the median key and all keys in the left node are
  less than the median key. We assume uniqueness of keys.

  Internal nodes contain keys as well as separators. They also contain pointers to child nodes.
  Internal nodes and leaf nodes may have differrent max_elem boundaries.
  Rebalancing happens when a node is full and we need to insert a new key into it.
 */
class BTreeNode {
  using _Key = uint64_t;

  friend class ::dfly::BTree;

  BTreeNode(bool leaf) : val_(0) {
    leaf_ = leaf;
  }

  static constexpr uint16_t kKeySize = sizeof(_Key);
  static constexpr uint16_t kTargetNodeSize = 256;
  static constexpr uint16_t kKeyOffset = sizeof(uint64_t);  // val_
  static constexpr uint16_t kMaxLeafKeys = (kTargetNodeSize - kKeyOffset) / kKeySize;
  static constexpr uint16_t kMinLeafKeys = kMaxLeafKeys / 2;

  static_assert(kMaxLeafKeys < 64, "num_items_ is 6 bits");

  // internal node:
  // x slots, (x+1) children: x * kKeySize + (x+1) * sizeof(BTreeNode*) = x * (kKeySize + 8) + 8
  // x = (kTargetNodeSize - 8 - kKeyOffset) / (kKeySize + 8)
  static constexpr uint16_t kMaxInnerKeys = (kTargetNodeSize - 8 - kKeyOffset) / (kKeySize + 8);
  static constexpr uint16_t kMinInnerKeys = kMaxInnerKeys / 2;

  static constexpr uint16_t kChildOffset =
      kMaxInnerKeys * kKeySize + kKeyOffset;  // aligned to 8 bytes.

 public:
  using Key_t = _Key;

  void InitSingle(Key_t key) {
    SetKey(0, key);
    num_items_ = 1;
  }

  Key_t Key(unsigned index) const {
    return absl::little_endian::Load64(KeyPtr(index));
  }

  void SetKey(size_t index, Key_t item) {
    uint8_t* slot = KeyPtr(index);
    absl::little_endian::Store64(slot, item);
  }

  BTreeNode** Children() {
    uint8_t* ptr = reinterpret_cast<uint8_t*>(this) + kChildOffset;
    return reinterpret_cast<BTreeNode**>(ptr);
  }

  BTreeNode* Child(unsigned i) {
    return Children()[i];
  }

  void SetChild(unsigned i, BTreeNode* child) {
    Children()[i] = child;
  }

  struct SearchResult {
    uint32_t index : 31;
    uint32_t found : 1;
  };

  SearchResult BSearch(Key_t key) const;

  Key_t SwapKey(size_t index, Key_t item) {
    uint8_t* ptr = KeyPtr(index);
    Key_t old_item = absl::little_endian::Load64(ptr);
    absl::little_endian::Store64(ptr, item);
    return old_item;
  }

  void Split(BTreeNode* right, Key_t* median);

  bool IsLeaf() const {
    return leaf_;
  }

  unsigned NumItems() const {
    return num_items_;
  }

  unsigned AvailableSlotCount() const {
    return MaxItems() - num_items_;
  }

  unsigned MaxItems() const {
    return leaf_ ? kMaxLeafKeys : kMaxInnerKeys;
  }

  unsigned MinItems() const {
    return leaf_ ? kMinLeafKeys : kMinInnerKeys;
  }

  bool IsLeafFull() const {
    return num_items_ >= kMaxLeafKeys;
  }

  bool IsInnerFull() const {
    return num_items_ >= kMaxInnerKeys;
  }

  void ShiftRight(unsigned index);

  void ShiftLeft(unsigned index, bool child_step_right = false);

  void LeafEraseRight() {
    assert(IsLeaf() && num_items_ > 0);
    --num_items_;
  }

  // Rebalance a full child at position pos, at which we tried to insert at insert_pos.
  // Returns the node and the position to insert into if rebalancing successful.
  // Returns nullptr if rebalancing did not succeed.
  std::pair<BTreeNode*, unsigned> RebalanceChild(unsigned pos, unsigned insert_pos);

  void RebalanceChildToLeft(unsigned child_pos, unsigned count);
  void RebalanceChildToRight(unsigned child_pos, unsigned count);

  // Inserts item into a leaf node.
  // Assumes: the node is IsLeaf() and !IsLeafFull().
  void LeafInsert(unsigned index, Key_t item) {
    assert(IsLeaf() && !IsLeafFull());
    InsertItem(index, item);
  }

  void InnerInsert(unsigned index, Key_t item, BTreeNode* child) {
    InsertItem(index, item);
    SetChild(index + 1, child);
  }

  // Tries to merge the child at position pos with its sibling.
  // If we did not succeed to merge, we try to rebalance.
  // Returns retired BTreeNode* if children got merged and this parent node's children
  // count decreased, otherwise, we return nullptr (rebalanced).
  BTreeNode* MergeOrRebalanceChild(unsigned pos);

  void Validate(Key_t upper_bound) const;

 private:
  void MergeFromRight(Key_t key, BTreeNode* right);

  void InsertItem(unsigned index, Key_t item) {
    assert(index <= num_items_);
    assert(index == 0 || Key(index - 1) < item);
    assert(index == num_items_ || Key(index) > item);

    ShiftRight(index);
    SetKey(index, item);
  }

  uint8_t* KeyPtr(unsigned index) {
    uint8_t* ptr = reinterpret_cast<uint8_t*>(this) + kKeyOffset + kKeySize * index;
    return ptr;
  }

  const uint8_t* KeyPtr(unsigned index) const {
    const uint8_t* ptr = reinterpret_cast<const uint8_t*>(this) + kKeyOffset + kKeySize * index;
    return ptr;
  }

  union {
    struct {
      uint8_t num_items_ : 6;
      uint8_t leaf_ : 1;
    };

    uint64_t val_;
  };
};

// Contains parent/index pairs. Meaning that node0->Child(index0) == node1.
class BTreePath {
  static constexpr unsigned kMaxDepth = 16;

 public:
  void Push(BTreeNode* node, unsigned pos) {
    assert(depth_ < kMaxDepth);
    record_[depth_].node = node;
    record_[depth_].pos = pos;
    depth_++;
  }

  unsigned Depth() const {
    return depth_;
  }

  std::pair<BTreeNode*, unsigned> Last() const {
    return {record_[depth_ - 1].node, record_[depth_ - 1].pos};
  }

  BTreeNode* Node(unsigned i) const {
    assert(i < depth_);
    return record_[i].node;
  }

  unsigned Position(unsigned i) const {
    assert(i < depth_);
    return record_[i].pos;
  }

  void Pop() {
    assert(depth_ > 0u);
    depth_--;
  }

  // Extend the path to the leaf by always taking the leftmost child.
  void DigRight() {
    assert(depth_ > 0u && !Last().first->IsLeaf());
    BTreeNode* last = Last().first;
    do {
      unsigned pos = last->NumItems();
      BTreeNode* child = last->Child(last->NumItems());
      Push(child, pos);
      last = child;
    } while (!last->IsLeaf());
  }

 private:
  struct Record {
    BTreeNode* node;
    unsigned pos;
  };

  Record record_[kMaxDepth];
  unsigned depth_ = 0;
};

}  // namespace detail

class BTree {
  BTree(const BTree&) = delete;
  BTree& operator=(const BTree&) = delete;

  using BTreeNode = detail::BTreeNode;
  using BTreePath = detail::BTreePath;

 public:
  using Key_t = detail::BTreeNode::Key_t;

  BTree(PMR_NS::memory_resource* mr = PMR_NS::get_default_resource()) : mr_(mr) {
  }

  // true if inserted, false if skipped.
  bool Insert(Key_t item);

  bool Contains(Key_t item) const;

  bool Delete(Key_t item);

  Key_t PopMin();

  size_t Height() const {
    return height_;
  }

  size_t Size() const {
    return count_;
  }

  size_t NodeCount() const {
    return num_nodes_;
  }

  void Clear();

  void Validate();

 private:
  BTreeNode* CreateNode(bool leaf);

  void DestroyNode(BTreeNode* node);

  // Unloads the full leaf to allow insertion of additional item.
  // The leaf should be the last one in the path.
  std::pair<BTreeNode*, Key_t> InsertFullLeaf(Key_t item, const BTreePath& path);

  // Charts the path towards key. Returns true if key is found.
  // In that case path->Last().first->Key(path->Last().second) == key.
  // Fills the tree path not including the key itself.
  bool Locate(Key_t key, BTreePath* path) const;

  BTreeNode* root_ = nullptr;  // root node or NULL if empty tree
  uint32_t count_ = 0;         // number of items in tree
  uint32_t height_ = 0;        // height of tree from root to leaf
  uint32_t num_nodes_ = 0;     // number of nodes in tree
  PMR_NS::memory_resource* mr_;
};

}  // namespace dfly
