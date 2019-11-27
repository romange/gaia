// Copyright 2013, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "file/file.h"

#include <gmock/gmock.h>
#include <memory>

#include "base/gtest.h"
#include "base/logging.h"
#include "file/file_util.h"
#include "file/filesource.h"
#include "file/gzip_file.h"
#include "file/lz4_file.h"
#include "file/test_util.h"

#include "util/sinksource.h"
#include "util/zlib_source.h"

using testing::ElementsAre;

using std::string;

namespace file {
using util::StatusObject;

class FileTest : public ::testing::Test {
 protected:
};

TEST_F(FileTest, CompressGzipFile) {
  string file_path = "/tmp/gzip_test.txt";
  file_util::WriteStringToFileOrDie("Test\n\nFoo\nBar\n", file_path);
  file_util::CompressToGzip(file_path);
}

TEST_F(FileTest, GzipFile) {
  string file_path = "/tmp/gzip_test.txt.gz";
  WriteFile* file = GzipFile::Create(file_path, 9);
  ASSERT_TRUE(file->Open());

  string data(1 << 16, 'a');

  auto status = file->Write(data);
  ASSERT_TRUE(status.ok()) << status;
  ASSERT_TRUE(file->Close());
}

TEST_F(FileTest, WriteFile) {
  string file_path = "/tmp/write_file.txt";
  WriteFile* file = Open(file_path);
  ASSERT_TRUE(file != nullptr);

  string data(1 << 16, 'a');

  auto status = file->Write(data);

  ASSERT_TRUE(status.ok()) << status;
  ASSERT_TRUE(file->Close());
}

TEST_F(FileTest, WriteFileToDir) {
  string file_path = "/tmp";
  testing::internal::CaptureStderr();
  WriteFile* file = Open(file_path);
  ASSERT_TRUE(file == nullptr);
  EXPECT_FALSE(testing::internal::GetCapturedStderr().empty());
}

TEST_F(FileTest, Append) {
  const string kFileName = "/tmp/write_file.txt";
  WriteFile* file = Open(kFileName);
  ASSERT_TRUE(file != nullptr);

  string data("foo");
  auto status = file->Write(data);
  ASSERT_TRUE(status.ok()) << status;
  ASSERT_TRUE(file->Close());

  OpenOptions opts;
  opts.append = true;
  file = Open(kFileName, opts);
  ASSERT_TRUE(file != nullptr);

  status = file->Write("bar");
  ASSERT_TRUE(file->Close());
  ASSERT_TRUE(file_util::ReadFileToString(kFileName, &data));
  EXPECT_EQ("foobar", data);
}

TEST_F(FileTest, AppendNonExisting) {
  string file_name = file_util::TempFile::TempFilename("/tmp");
  OpenOptions opts;
  opts.append = true;
  WriteFile* file = Open(file_name, opts);
  ASSERT_TRUE(file != nullptr);
  ASSERT_TRUE(file->Close());
}

TEST_F(FileTest, LZ4File) {
  string file_path = "/tmp/lz4_test.txt.lz4";
  WriteFile* file = LZ4File::Create(file_path, 9);
  ASSERT_TRUE(file->Open());

  string data(1 << 16, 'a');

  auto status = file->Write(data);
  ASSERT_TRUE(status.ok()) << status;
  ASSERT_TRUE(file->Close());
}

TEST_F(FileTest, CopyFile) {
  string file_path = "/tmp/copyfile.txt", dest_path = "/tmp/copyfile2.txt";
  file_util::WriteStringToFileOrDie("Test", file_path);
  file_util::CopyFileOrDie(file_path.c_str(), dest_path.c_str());

  string data;
  file_util::ReadFileToStringOrDie(dest_path, &data);
  EXPECT_EQ("Test", data);
}

TEST_F(FileTest, UniquePtr) {
  std::unique_ptr<WriteFile> file(Open(base::GetTestTempPath("foo.txt")));
}

constexpr size_t kStrLen = 1 << 17;

static void BM_GZipFile(benchmark::State& state) {
  std::string data = base::RandStr(kStrLen);
  string file_path = "/tmp/gzip_test.txt.gz";

  while (state.KeepRunning()) {
    WriteFile* file = GzipFile::Create("/dev/null", 1);
    CHECK(file->Open());
    for (unsigned i = 0; i < state.range(0); ++i)
      CHECK_STATUS(file->Write(data));
    CHECK(file->Close());
  }
}
BENCHMARK(BM_GZipFile)->Range(8, 32);

static void BM_ZipSink(benchmark::State& state) {
  std::string data = base::RandStr(kStrLen);

  while (state.KeepRunning()) {
    util::StringSink* str = new util::StringSink;
    util::ZlibSink zsink(str, 1);
    for (unsigned i = 0; i < state.range(0); ++i) {
      CHECK_STATUS(zsink.Append(strings::ToByteRange(data)));
    }
    CHECK_STATUS(zsink.Flush());
    file_util::WriteStringToFileOrDie(str->contents(), "/dev/null");
  }
}
BENCHMARK(BM_ZipSink)->Range(8, 32);

static void BM_LineReader(benchmark::State& state) {
  string buffer;
  const size_t line_sz = state.range(0);
  for (size_t i = 0; i < 1000; ++i) {
    buffer.append(string(line_sz, 'a')).append("\n");
  }
  RingSource rs(97, buffer);
  LineReader lr(&rs, DO_NOT_TAKE_OWNERSHIP);
  StringPiece line;

  while (state.KeepRunning()) {
    CHECK(lr.Next(&line));
    CHECK_EQ(line_sz, line.size());
  }
}
BENCHMARK(BM_LineReader)->Range(100, 4000);


}  // namespace file
