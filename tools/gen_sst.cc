#include <stdio.h>

#include <memory>
#include <string>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/sst_file_writer.h"
#include "util/gflags_compat.h"
#include "util/random.h"
#include "util/string_util.h"

using GFLAGS_NAMESPACE::ParseCommandLineFlags;

DEFINE_string(op, "ingest", "gen or ingest");
DEFINE_string(db_path, "/Users/yiwu/home/tmp/db", "");
DEFINE_string(temp_path, "/Users/yiwu/home/tmp/sst", "");
DEFINE_int32(key_prefix_bytes, 20, "");
DEFINE_int32(key_suffix_bytes, 10, "");
DEFINE_int32(num_files, 200, "");

using namespace rocksdb;
  
Status GenSST() {
  Status s;
  Random r(337);
  size_t key_size = FLAGS_key_prefix_bytes + FLAGS_key_suffix_bytes;
  Options opt;
  for (int i = 0; i < FLAGS_num_files; i++) {
    std::unique_ptr<SstFileWriter> writer(
        new SstFileWriter(EnvOptions(), opt));
    char buf[100];
    snprintf(buf, sizeof(buf), "%06d", i + 1);
    s = writer->Open(FLAGS_temp_path + "/" + std::string(buf) + ".sst");
    if (!s.ok()) {
      break;
    }
    char key_buf1[key_size];
    char key_buf2[key_size];
    for (size_t p = 0; p < key_size; p++) {
      key_buf1[p] = static_cast<char>(r.Next() % 256);
    }
    memcpy(key_buf2, key_buf1, key_size);
    for (size_t p = FLAGS_key_prefix_bytes; p < key_size; p++) {
      key_buf2[p] = static_cast<char>(r.Next() % 256);
    }
    Slice key1(key_buf1, key_size);
    Slice key2(key_buf2, key_size);
    if (key1.ToString() > key2.ToString()) {
      key1 = Slice(key_buf2, key_size);
      key2 = Slice(key_buf1, key_size);
    }
    s = writer->Put(key1, "");
    if (!s.ok()) {
      break;
    }
    s = writer->Put(key2, "");
    if (!s.ok()) {
      break;
    }
    s = writer->Finish();
    if (!s.ok()) {
      break;
    }
  }
  return s;
}

Status IngestSST() {
  DB* db = nullptr;
  Options opt;
  opt.create_if_missing = true;
  opt.max_open_files = 200;
  Status s = DB::Open(opt, FLAGS_db_path, &db);
  if (!s.ok()) {
    return s;
  }
  for (int i = 0; i < FLAGS_num_files; i++) {
    char buf[100];
    snprintf(buf, sizeof(buf), "%06d", i + 1);
    std::vector<std::string> files = {
      FLAGS_temp_path + "/" + std::string(buf) + ".sst"};
    s = db->IngestExternalFile(files, IngestExternalFileOptions());
    if (!s.ok()) {
      return s;
    }
  }
  delete db;
  return s;
}

int main(int argc, char** argv) {
  ParseCommandLineFlags(&argc, &argv, true);
  Status s;
  if (FLAGS_op == "gen") {
    s = GenSST();
    if (!s.ok()) {
      printf("GenSST: %s\n", s.ToString().c_str());
      return 0;
    }
  }
  if (FLAGS_op == "ingest") {
    s = IngestSST();
    if (!s.ok()) {
      printf("IngestSST: %s\n", s.ToString().c_str());
    }
  }
  return 0;
}
