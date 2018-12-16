// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include <unordered_map>

#include <gperftools/malloc_extension.h>
#include <gperftools/profiler.h>
#include <gperftools/heap-profiler.h>

#include "base/walltime.h"
#include "base/logging.h"
#include "strings/human_readable.h"
#include "strings/numbers.h"
#include "strings/split.h"
#include "strings/strcat.h"
#include "util/spawn.h"
#include "util/http/http_conn_handler.h"
#include "util/fibers_ext.h"

namespace util {
namespace http {
namespace {
char last_profile_suffix[100] = {0};
}

using namespace std;
using namespace boost;
using beast::http::field;
namespace h2 = beast::http;
typedef h2::response<h2::string_body> StringResponse;

static void HandleCpuProfile(bool enable, StringResponse* response) {
  string profile_name = "/tmp/" + base::ProgramBaseName();
  response->set(h2::field::cache_control, "no-cache, no-store, must-revalidate");
  response->set(h2::field::pragma, "no-cache");
  response->set(field::content_type, kHtmlMime);

  auto& body = response->body();

  if (enable) {
    if (last_profile_suffix[0]) {
      body.append("<p> Yo, already profiling, stupid!</p>\n");
    } else {
      string suffix = base::LocalTimeNow("_%d%m%Y_%H%M%S.prof");
      profile_name.append(suffix);
      strcpy(last_profile_suffix, suffix.c_str());
      int res = ProfilerStart(profile_name.c_str());
      LOG(INFO) << "Starting profiling into " << profile_name << " " << res;
      body.append("<p> Yeah, let's profile this bitch, baby!</p> \n"
        "<img src='https://gist.githack.com/romange/4760c3eebc407755f856fec8e5b6d4c1/raw/profiler.gif'>\n");
    }
    return;
  }
  ProfilerStop();
  if (last_profile_suffix[0] == '\0') {
    body.append("<h3>Profiling is off, commander!</h3> \n");
    return;
  }
  string cmd("nice -n 15 pprof -noinlines -lines -unit ms --svg ");
  string symbols_name = base::ProgramAbsoluteFileName() + ".debug";
  LOG(INFO) << "Symbols " << symbols_name << ", suffix: " << last_profile_suffix;
  if (access(symbols_name.c_str(), R_OK) != 0) {
    symbols_name = base::ProgramAbsoluteFileName();
  }
  cmd.append(symbols_name).append(" ");

  profile_name.append(last_profile_suffix);
  cmd.append(profile_name).append(" > ");

  string err_log = profile_name + ".err";
  profile_name.append(".svg");

  cmd.append(profile_name).append(" 2> ").append(err_log);

  LOG(INFO) << "Running command: " << cmd;
  last_profile_suffix[0] = '\0';

  int sh_res = util::sh_exec(cmd.c_str());
  if (sh_res != 0) {
    LOG(ERROR) << "Error running sh_exec, status: " << errno << " " << strerror(errno);
  }

  // Redirect browser to show this file.
  string url("filez?file=");
  url.append(profile_name);
  LOG(INFO) << "Redirecting to " << url;
  google::FlushLogFiles(google::INFO);

  response->set(h2::field::location, url);
  response->result(h2::status::moved_permanently);
}


void ProfilezHandler(const QueryArgs& args, HttpHandler::SendFunction* send) {
  bool enable = false;
  for (const auto& k_v : args) {
    if (k_v.first == "profile" && k_v.second == "on")
      enable = true;
  }

  fibers_ext::Done done;
  std::thread([=, &done] {
    StringResponse response;

    HandleCpuProfile(enable, &response);
    send->Invoke(std::move(response));
    done.Notify();
  }).detach();

  // Instead of joining the thread which is not fiber-friendly,
  // I use done to block the fiber but free the thread to handle other fibers.
  // Still this fiber connection is blocked.
  done.Wait();
}

#if 0
static void HandleHeapProfile(bool enable, Response* response) {
  string profile_name = "/tmp/" + base::ProgramBaseName();
  response->AddHeader("Cache-Control", "no-cache, no-store, must-revalidate");
  response->AddHeader("Pragma", "no-cache");

  if (enable) {
    if (IsHeapProfilerRunning()) {
      response->AppendContent("<p> Man, heap profiling is already running, relax!</p>\n");
    } else {
      string suffix = LocalTimeNow("_%d%m%Y_%H%M%S");
      profile_name.append(suffix);
      HeapProfilerStart(profile_name.c_str());
      LOG(INFO) << "Starting heap profiling into " << profile_name;
      response->AppendContent("<p> Let's find memory leaks, w00t!</p> \n");
    }
    return;
  }

  HeapProfilerStop();
  response->AppendContent("<h3>Heap profiling is off, master!</h3> \n"
    "<img src='https://m.popkey.co/770e6b/4Mm5x.gif'>\n");
  return;
}

static string HumanReadableBytes(StringPiece key, int64 val) {
  return StrCat(key, ": ", HumanReadableNumBytes::ToString(val), "\n");
}

#endif

}  // namespace http
}  // namespace util

