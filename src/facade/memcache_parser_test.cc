// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "facade/memcache_parser.h"

#include <gmock/gmock.h>

#include "absl/strings/str_cat.h"
#include "base/gtest.h"
#include "base/logging.h"
#include "facade/facade_test.h"

using namespace testing;
using namespace std;

namespace facade {

class MCParserTest : public testing::Test {
 protected:
  MemcacheParser parser_;
  MemcacheParser::Command cmd_;
  uint32_t consumed_;

  unique_ptr<uint8_t[]> stash_;
};

TEST_F(MCParserTest, Basic) {
  MemcacheParser::Result st = parser_.Parse("set a 1 20 3\r\n", &consumed_, &cmd_);
  EXPECT_EQ(MemcacheParser::OK, st);
  EXPECT_EQ("a", cmd_.key);
  EXPECT_EQ(1, cmd_.flags);
  EXPECT_EQ(20, cmd_.expire_ts);
  EXPECT_EQ(3, cmd_.bytes_len);
  EXPECT_EQ(MemcacheParser::SET, cmd_.type);

  st = parser_.Parse("quit\r\n", &consumed_, &cmd_);
  EXPECT_EQ(MemcacheParser::OK, st);
  EXPECT_EQ(MemcacheParser::QUIT, cmd_.type);
}

TEST_F(MCParserTest, Incr) {
  MemcacheParser::Result st = parser_.Parse("incr a\r\n", &consumed_, &cmd_);
  EXPECT_EQ(MemcacheParser::PARSE_ERROR, st);

  st = parser_.Parse("incr a 1\r\n", &consumed_, &cmd_);
  EXPECT_EQ(MemcacheParser::OK, st);
  EXPECT_EQ(MemcacheParser::INCR, cmd_.type);
  EXPECT_EQ("a", cmd_.key);
  EXPECT_EQ(1, cmd_.delta);
  EXPECT_FALSE(cmd_.no_reply);

  st = parser_.Parse("incr a -1\r\n", &consumed_, &cmd_);
  EXPECT_EQ(MemcacheParser::BAD_DELTA, st);

  st = parser_.Parse("decr b 10 noreply\r\n", &consumed_, &cmd_);
  EXPECT_EQ(MemcacheParser::OK, st);
  EXPECT_EQ(MemcacheParser::DECR, cmd_.type);
  EXPECT_EQ(10, cmd_.delta);
}

TEST_F(MCParserTest, Stats) {
  MemcacheParser::Result st = parser_.Parse("stats foo\r\n", &consumed_, &cmd_);
  EXPECT_EQ(MemcacheParser::OK, st);
  EXPECT_EQ(consumed_, 11);
  EXPECT_EQ(cmd_.type, MemcacheParser::STATS);
  EXPECT_EQ("foo", cmd_.key);

  cmd_ = MemcacheParser::Command{};
  st = parser_.Parse("stats  \r\n", &consumed_, &cmd_);
  EXPECT_EQ(MemcacheParser::OK, st);
  EXPECT_EQ(consumed_, 9);
  EXPECT_EQ(cmd_.type, MemcacheParser::STATS);
  EXPECT_EQ("", cmd_.key);

  cmd_ = MemcacheParser::Command{};
  st = parser_.Parse("stats  fpp bar\r\n", &consumed_, &cmd_);
  EXPECT_EQ(MemcacheParser::PARSE_ERROR, st);
}

TEST_F(MCParserTest, NoreplyBasic) {
  MemcacheParser::Result st = parser_.Parse("set mykey 1 2 3 noreply\r\n", &consumed_, &cmd_);

  EXPECT_EQ(MemcacheParser::OK, st);
  EXPECT_EQ("mykey", cmd_.key);
  EXPECT_EQ(1, cmd_.flags);
  EXPECT_EQ(2, cmd_.expire_ts);
  EXPECT_EQ(3, cmd_.bytes_len);
  EXPECT_EQ(MemcacheParser::SET, cmd_.type);
  EXPECT_TRUE(cmd_.no_reply);

  cmd_ = MemcacheParser::Command{};
  st = parser_.Parse("set mykey2 4 5 6\r\n", &consumed_, &cmd_);

  EXPECT_EQ(MemcacheParser::OK, st);
  EXPECT_EQ("mykey2", cmd_.key);
  EXPECT_EQ(4, cmd_.flags);
  EXPECT_EQ(5, cmd_.expire_ts);
  EXPECT_EQ(6, cmd_.bytes_len);
  EXPECT_EQ(MemcacheParser::SET, cmd_.type);
  EXPECT_FALSE(cmd_.no_reply);
}

class MCParserNoreplyTest : public MCParserTest {
 protected:
  void RunTest(string_view str, bool noreply) {
    MemcacheParser::Result st = parser_.Parse(str, &consumed_, &cmd_);

    EXPECT_EQ(MemcacheParser::OK, st);
    EXPECT_EQ(cmd_.no_reply, noreply);
  }
};

TEST_F(MCParserNoreplyTest, StoreCommands) {
  RunTest("set mykey 0 0 3 noreply\r\n", true);
  RunTest("set mykey 0 0 3\r\n", false);
  RunTest("add mykey 0 0 3\r\n", false);
  RunTest("replace mykey 0 0 3\r\n", false);
  RunTest("append mykey 0 0 3\r\n", false);
  RunTest("prepend mykey 0 0 3\r\n", false);
}

TEST_F(MCParserNoreplyTest, Other) {
  RunTest("quit\r\n", false);
  RunTest("delete mykey\r\n", false);
  RunTest("incr mykey 1\r\n", false);
  RunTest("decr mykey 1\r\n", false);
  RunTest("flush_all\r\n", false);
}

}  // namespace facade
