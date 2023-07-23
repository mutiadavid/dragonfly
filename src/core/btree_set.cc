// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/btree_set.h"

#include <utility>
#include <vector>

#include "base/logging.h"

namespace dfly {
using namespace std;

namespace detail {

[[maybe_unused]] constexpr size_t kSzBTreeNode = sizeof(BTreeNode);

static int KeyCompare(BTreeNode::Key_t a, BTreeNode::Key_t b) {
  if (a < b)
    return -1;

  return a > b ? 1 : 0;
}

// Returns the position of the first item whose key is greater or equal than key.
// if all items are smaller than key, returns num_items_.
auto BTreeNode::BSearch(Key_t key) const -> SearchResult {
  uint32_t i = 0;
  uint32_t n = num_items_;
  while (i < n) {
    uint32_t j = (i + n) >> 1;
    DCHECK_LT(j, n);

    Key_t item = Key(j);
    int cmp = KeyCompare(key, item);
    if (cmp == 0) {
      return SearchResult{.index = j, .found = 1};
    }

    if (cmp < 0) {
      n = j;
    } else {
      i = j + 1;  // we never return indices upto j because they are strictly less than key.
    }
  }
  DCHECK_EQ(i, n);

  return {.index = i, .found = 0};
}

void BTreeNode::ShiftRight(unsigned index) {
  unsigned num_items_to_shift = num_items_ - index;
  if (num_items_to_shift > 0) {
    uint8_t* ptr = KeyPtr(index);
    memmove(ptr + kKeySize, ptr, num_items_to_shift * kKeySize);
    BTreeNode** children = Children();
    if (!leaf_) {
      memmove(children + index + 1, children + index,
              (num_items_to_shift + 1) * sizeof(BTreeNode*));
    }
  }
  num_items_++;
}

void BTreeNode::ShiftLeft(unsigned index, bool child_step_right) {
  DCHECK_LT(index, num_items_);

  unsigned num_items_to_shift = num_items_ - index - 1;
  if (num_items_to_shift > 0) {
    memmove(KeyPtr(index), KeyPtr(index + 1), num_items_to_shift * kKeySize);
    if (!leaf_) {
      index += unsigned(child_step_right);
      num_items_to_shift = num_items_ - index;
      if (num_items_to_shift > 0) {
        BTreeNode** children = Children();
        memmove(children + index, children + index + 1, num_items_to_shift * sizeof(BTreeNode*));
      }
    }
  }
  num_items_--;
}

void BTreeNode::Split(BTreeNode* right, Key_t* median) {
  unsigned mid = num_items_ / 2;
  *median = Key(mid);
  right->leaf_ = leaf_;
  right->num_items_ = num_items_ - (mid + 1);
  memmove(right->KeyPtr(0), KeyPtr(mid + 1), right->num_items_ * kKeySize);
  if (!leaf_) {
    BTreeNode** rchild = right->Children();
    for (size_t i = 0; i <= right->num_items_; i++) {
      rchild[i] = Child(mid + 1 + i);
    }
  }
  num_items_ = mid;
}

std::pair<BTreeNode*, unsigned> BTreeNode::RebalanceChild(unsigned pos, unsigned insert_pos) {
  unsigned to_move = 0;
  BTreeNode* node = Child(pos);

  if (pos > 0) {
    BTreeNode* left = Child(pos - 1);
    unsigned dest_free = left->AvailableSlotCount();
    if (dest_free > 0) {
      // We bias rebalancing based on the position being inserted. If we're
      // inserting at the end of the right node then we bias rebalancing to
      // fill up the left node.
      if (insert_pos == node->NumItems()) {
        to_move = dest_free;
        DCHECK_LT(to_move, node->NumItems());
      } else if (dest_free > 1) {
        // we move less than left free capacity which leaves as some space in the node.
        to_move = dest_free / 2;
      }

      if (to_move) {
        unsigned dest_old_count = left->NumItems();
        RebalanceChildToLeft(pos, to_move);
        DCHECK(node->AvailableSlotCount() == to_move);
        if (insert_pos < to_move) {
          DCHECK_GT(left->AvailableSlotCount(), 0u);     // we did not fill up the left node.
          insert_pos = dest_old_count + insert_pos + 1;  // +1 because we moved the separator.
          node = left;
        } else {
          insert_pos -= to_move;
        }

        return {node, insert_pos};
      }
    }
  }

  if (pos < NumItems()) {
    BTreeNode* right = Child(pos + 1);
    unsigned dest_free = right->AvailableSlotCount();
    if (dest_free > 0) {
      if (insert_pos == 0) {
        to_move = dest_free;
        DCHECK_LT(to_move, node->NumItems());
      } else if (dest_free > 1) {
        to_move = dest_free / 2;
      }

      if (to_move) {
        RebalanceChildToRight(pos, to_move);
        if (insert_pos > node->NumItems()) {
          insert_pos -= (node->NumItems() + 1);
          node = right;
        }
        return {node, insert_pos};
      }
    }
  }
  return {nullptr, 0};
}

void BTreeNode::RebalanceChildToLeft(unsigned child_pos, unsigned count) {
  DCHECK_GT(child_pos, 0u);
  BTreeNode* src = Child(child_pos);
  BTreeNode* dest = Child(child_pos - 1);
  DCHECK_GE(src->NumItems(), count);
  DCHECK_GE(count, 1u);
  DCHECK_GE(dest->AvailableSlotCount(), count);

  unsigned dest_items = dest->NumItems();

  // Move the delimiting value to the left node.
  dest->SetKey(dest_items, Key(child_pos - 1));

  // Copy src keys [0, count-1] to dest keys [dest_items+1, dest_items+count].
  for (unsigned i = 1; i < count; ++i) {
    dest->SetKey(dest_items + i, src->Key(i - 1));
  }

  SetKey(child_pos - 1, src->Key(count - 1));

  // Shift the values in the right node to their correct position.
  for (unsigned i = count; i < src->NumItems(); ++i) {
    src->SetKey(i - count, src->Key(i));
  }

  if (!src->IsLeaf()) {
    // Move the child pointers from the right to the left node.
    for (unsigned i = 0; i < count; ++i) {
      dest->SetChild(1 + dest->NumItems() + i, src->Child(i));
    }
    for (unsigned i = count; i <= src->NumItems(); ++i) {
      src->SetChild(i - count, src->Child(i));
      src->SetChild(i, NULL);
    }
  }

  // Fixup the counts on the src and dest nodes.
  dest->num_items_ += count;
  src->num_items_ -= count;

  // TODO: to remove this
  // dest->Validate(Key(child_pos - 1));
  // src->Validate(child_pos < NumItems() ? Key(child_pos) : UINT64_MAX);
}

void BTreeNode::RebalanceChildToRight(unsigned child_pos, unsigned count) {
  DCHECK_LT(child_pos, NumItems());
  BTreeNode* src = Child(child_pos);
  BTreeNode* dest = Child(child_pos + 1);

  DCHECK_GE(src->NumItems(), count);
  DCHECK_GE(count, 1u);
  DCHECK_GE(dest->AvailableSlotCount(), count);

  unsigned dest_items = dest->NumItems();

  DCHECK_GT(dest_items, 0u);

  // Shift the values in the right node to their correct position.
  for (int i = dest_items - 1; i >= 0; --i) {
    dest->SetKey(i + count, dest->Key(i));
  }

  // Move the delimiting value to the left node and the new delimiting value
  // from the right node.
  Key_t new_delim = src->Key(src->NumItems() - count);
  for (unsigned i = 1; i < count; ++i) {
    unsigned src_id = src->NumItems() - count + i;
    dest->SetKey(i - 1, src->Key(src_id));
  }
  // Move parent's delimiter to destination and update it with new delimiter.
  dest->SetKey(count - 1, Key(child_pos));
  SetKey(child_pos, new_delim);

  if (!src->IsLeaf()) {
    // Shift child pointers in the right node to their correct position.
    for (int i = dest_items; i >= 0; --i) {
      dest->SetChild(i + count, dest->Child(i));
    }

    // Move child pointers from the left node to the right.
    for (unsigned i = 0; i < count; ++i) {
      unsigned src_id = src->NumItems() - (count - 1) + i;
      dest->SetChild(i, src->Child(src_id));
      src->SetChild(src_id, NULL);
    }
  }

  // Fixup the counts on the src and dest nodes.
  dest->num_items_ += count;
  src->num_items_ -= count;
}

BTreeNode* BTreeNode::MergeOrRebalanceChild(unsigned pos) {
  BTreeNode* node = Child(pos);
  BTreeNode* left = nullptr;

  DCHECK_GE(NumItems(), 1u);
  DCHECK_LT(node->NumItems(), node->MinItems());

  if (pos > 0) {
    left = Child(pos - 1);
    if (left->NumItems() + 1 + node->NumItems() <= left->MaxItems()) {
      left->MergeFromRight(Key(pos - 1), node);
      ShiftLeft(pos - 1, true);
      return node;
    }
  }

  if (pos < NumItems()) {
    BTreeNode* right = Child(pos + 1);
    if (node->NumItems() + 1 + right->NumItems() <= right->MaxItems()) {
      node->MergeFromRight(Key(pos), right);
      ShiftLeft(pos, true);
      return right;
    }

    // Try rebalancing with our right sibling.
    // TODO: don't perform rebalancing if
    // we deleted the first element from node and the node is not
    // empty. This is a small optimization for the common pattern of deleting
    // from the front of the tree.
    if (true) {
      unsigned to_move = (right->NumItems() - node->NumItems()) / 2;
      DCHECK_LT(to_move, right->NumItems());

      RebalanceChildToLeft(pos + 1, to_move);
      return nullptr;
    }
  }

  DCHECK(left);

  if (left) {
    // Try rebalancing with our left sibling.
    // TODO: don't perform rebalancing if we deleted the last element from node and the
    // node is not empty. This is a small optimization for the common pattern of deleting
    // from the back of the tree.
    if (true) {
      unsigned to_move = (left->NumItems() - node->NumItems()) / 2;
      DCHECK_LT(to_move, left->NumItems());
      RebalanceChildToRight(pos - 1, to_move);
      return nullptr;
    }
  }
  return nullptr;
}

void BTreeNode::MergeFromRight(Key_t key, BTreeNode* right) {
  DCHECK_LE(NumItems() + 1 + right->NumItems(), MaxItems());

  unsigned dest_items = NumItems();
  SetKey(dest_items, key);
  for (unsigned i = 0; i < right->NumItems(); ++i) {
    SetKey(dest_items + 1 + i, right->Key(i));
  }

  if (!IsLeaf()) {
    for (unsigned i = 0; i <= right->NumItems(); ++i) {
      SetChild(dest_items + 1 + i, right->Child(i));
    }
  }
  num_items_ += 1 + right->NumItems();
  right->num_items_ = 0;
}

void BTreeNode::Validate(Key_t upper_bound) const {
  CHECK_GE(NumItems(), 1u);
  for (unsigned i = 1; i < NumItems(); ++i) {
    CHECK_LT(Key(i - 1), Key(i));
  }
  CHECK_LT(Key(NumItems() - 1), upper_bound);
}

}  // namespace detail

detail::BTreeNode::Key_t BTree::PopMin() {
  CHECK(root_);

  BTreeNode* node = root_;
  BTreePath path;
  while (!node->IsLeaf()) {
    path.Push(node, 0);
    node = node->Child(0);
  }

  Key_t item = node->Key(0);
  node->ShiftLeft(0);
  count_--;

  // TODO: do not rebalance/merge immediately for popmin/max.
  if (node->NumItems() < BTreeNode::kMinLeafKeys) {
    if (node == root_) {
      if (node->NumItems() == 0) {
        if (node->IsLeaf()) {
          DCHECK_EQ(count_, 0u);
          root_ = nullptr;
        } else {
          root_ = root_->Child(0);
        }
        DestroyNode(node);
      }
      return item;
    }
    DCHECK_GT(path.Depth(), 0u);
    BTreeNode* parent = path.Last().first;
    BTreeNode* right = parent->Child(1);
    parent->RebalanceChildToLeft(1, 1 /*TBD*/);
  }
  return item;
}

bool BTree::Delete(Key_t item) {
  if (!root_)
    return false;

  BTreePath path;
  bool found = Locate(item, &path);
  if (!found)
    return false;
  BTreeNode* node = path.Last().first;
  unsigned key_pos = path.Last().second;

  if (node->IsLeaf()) {
    node->ShiftLeft(key_pos);  // shift left everything after key_pos.
  } else {
    // Pop the rightmost key before the key_pos and copy it to key_pos.
    path.DigRight();

    BTreeNode* leaf = path.Last().first;
    node->SetKey(key_pos, leaf->Key(leaf->NumItems() - 1));
    leaf->LeafEraseRight();
    node = leaf;
  }
  count_--;

  while (node->NumItems() < node->MinItems()) {
    if (node == root_) {
      if (node->NumItems() == 0) {
        if (node->IsLeaf()) {
          DCHECK_EQ(count_, 0u);
          root_ = nullptr;
        } else {
          root_ = root_->Child(0);
        }
        DestroyNode(node);
      }
      return true;
    }

    DCHECK_GT(path.Depth(), 0u);
    path.Pop();

    BTreeNode* parent = path.Last().first;
    unsigned pos = path.Last().second;
    DCHECK(parent->Child(pos) == node);
    node = parent->MergeOrRebalanceChild(pos);
    if (node == nullptr)
      break;
    DestroyNode(node);
    node = parent;
  }

  return true;
}

void BTree::Clear() {
  if (!root_)
    return;

  BTreePath path;
  BTreeNode* node = root_;

  auto deep_left = [&](unsigned pos) {
    do {
      path.Push(node, pos);
      node = node->Child(pos);
      pos = 0;
    } while (!node->IsLeaf());
  };

  if (!root_->IsLeaf())
    deep_left(0);

  while (true) {
    DestroyNode(node);

    if (path.Depth() == 0) {
      break;
    }
    node = path.Last().first;
    unsigned pos = path.Last().second;
    path.Pop();
    if (pos < node->NumItems()) {
      deep_left(pos + 1);
    }
  }
  root_ = nullptr;
  height_ = count_ = 0;
}

auto BTree::InsertFullLeaf(Key_t item, const BTreePath& path) -> pair<BTreeNode*, Key_t> {
  DCHECK_GT(path.Depth(), 0u);

  BTreeNode* node = path.Last().first;
  DCHECK(node->IsLeaf() && node->IsLeafFull());
  unsigned insert_pos = path.Last().second;
  unsigned level = path.Depth() - 1;
  if (level > 0) {
    BTreeNode* parent = path.Node(level - 1);
    unsigned pos = path.Position(level - 1);
    DCHECK(parent->Child(pos) == node);

    pair<BTreeNode*, unsigned> rebalance_res = parent->RebalanceChild(pos, insert_pos);
    if (rebalance_res.first) {
      rebalance_res.first->LeafInsert(rebalance_res.second, item);
      return {nullptr, 0};
    }
  }

  Key_t median;
  BTreeNode* right = CreateNode(node->IsLeaf());
  node->Split(right, &median);

  DCHECK(!node->IsLeafFull());

  if (insert_pos <= node->NumItems()) {
    DCHECK_LT(item, median);
    node->LeafInsert(insert_pos, item);
  } else {
    DCHECK_GT(item, median);
    right->LeafInsert(insert_pos - node->NumItems() - 1, item);
  }

  // we now must add right to the paren if it exists.
  while (level-- > 0) {
    node = path.Node(level);            // level up, now node is parent.
    insert_pos = path.Position(level);  // insert_pos is position of node in parent.

    DCHECK(!node->IsLeaf() && insert_pos <= node->NumItems());

    if (node->IsInnerFull()) {
      if (level > 0) {
        BTreeNode* parent = path.Node(level - 1);
        unsigned node_pos = path.Position(level - 1);
        DCHECK(parent->Child(node_pos) == node);
        pair<BTreeNode*, unsigned> rebalance_res = parent->RebalanceChild(node_pos, insert_pos);
        if (rebalance_res.first) {
          rebalance_res.first->InnerInsert(rebalance_res.second, median, right);
          return {nullptr, 0};
        }
      }

      Key_t parent_median;
      BTreeNode* parent_right = CreateNode(false);
      node->Split(parent_right, &parent_median);
      DCHECK(!node->IsInnerFull());

      if (insert_pos <= node->NumItems()) {
        DCHECK_LT(median, parent_median);
        node->InnerInsert(insert_pos, median, right);
      } else {
        DCHECK_GT(median, parent_median);
        parent_right->InnerInsert(insert_pos - node->NumItems() - 1, median, right);
      }
      right = parent_right;
      median = parent_median;
    } else {
      node->InnerInsert(insert_pos, median, right);
      return {nullptr, 0};
    }
  }
  return {right, median};
}

bool BTree::Locate(Key_t key, BTreePath* path) const {
  DCHECK(root_);
  BTreeNode* node = root_;
  while (true) {
    BTreeNode::SearchResult res = node->BSearch(key);
    path->Push(node, res.index);
    if (res.found) {
      return true;
    }
    DCHECK_LE(res.index, node->NumItems());

    if (node->IsLeaf()) {
      break;
    }
    node = node->Child(res.index);
  }
  return false;
}

bool BTree::Insert(Key_t item) {
  if (!root_) {
    root_ = CreateNode(true);
    DCHECK(root_);

    root_->InitSingle(item);
    count_ = height_ = 1;

    return true;
  }

  BTreePath path;
  bool found = Locate(item, &path);

  if (found) {
    return false;
  }

  DCHECK_GT(path.Depth(), 0u);
  BTreeNode* leaf = path.Last().first;
  DCHECK(leaf->IsLeaf());

  if (leaf->IsLeafFull()) {
    unsigned root_free = root_->AvailableSlotCount();
    pair<BTreeNode*, Key_t> res = InsertFullLeaf(item, path);
    if (res.first) {
      DCHECK_EQ(root_free, 0u);
      BTreeNode* new_root = CreateNode(false);
      new_root->InitSingle(res.second);
      new_root->SetChild(0, root_);
      new_root->SetChild(1, res.first);
      root_ = new_root;
      height_++;
    }
  } else {
    unsigned pos = path.Last().second;
    leaf->LeafInsert(pos, item);
  }
  count_++;
  return true;
}

bool BTree::Contains(Key_t item) const {
  BTreePath path;
  bool found = Locate(item, &path);
  return found;
}

void BTree::Validate() {
  if (!root_)
    return;

  vector<pair<BTreeNode*, uint64_t>> stack;

  stack.emplace_back(root_, UINT64_MAX);

  while (!stack.empty()) {
    BTreeNode* node = stack.back().first;
    uint64_t ubound = stack.back().second;
    stack.pop_back();

    node->Validate(ubound);

    if (!node->IsLeaf()) {
      for (unsigned i = 0; i < node->NumItems(); ++i) {
        stack.emplace_back(node->Child(i), node->Key(i));
      }
      stack.emplace_back(node->Child(node->NumItems()), ubound);
    }
  }
}

detail::BTreeNode* BTree::CreateNode(bool leaf) {
  num_nodes_++;
  void* ptr = mr_->allocate(BTreeNode::kTargetNodeSize, 8);
  BTreeNode* node = new (ptr) BTreeNode(leaf);

  return node;
}

void BTree::DestroyNode(BTreeNode* node) {
  void* ptr = node;
  mr_->deallocate(ptr, BTreeNode::kTargetNodeSize, 8);
  num_nodes_--;
}

}  // namespace dfly
