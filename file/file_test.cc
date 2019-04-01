// Copyright 2013, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "file/file.h"

#include <memory>
#include <gmock/gmock.h>

#include "file/file_util.h"
#include "file/gzip_file.h"
#include "file/lz4_file.h"
#include "base/gtest.h"
#include "base/logging.h"

using testing::ElementsAre;

using std::string;
namespace file {

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


static void BM_GZipFile(benchmark::State& state) {
  std::string data = base::RandStr(state.range(0));
  string file_path = "/tmp/gzip_test.txt.gz";

  while (state.KeepRunning()) {
    WriteFile* file = GzipFile::Create(file_path, 1);
    CHECK(file->Open());
    CHECK_STATUS(file->Write(data));
    CHECK(file->Close());
  }
}
BENCHMARK(BM_GZipFile)->Arg(17);

}  // namespace file
