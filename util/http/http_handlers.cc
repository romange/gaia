// Copyright 2014, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include <gperftools/malloc_extension.h>
#include <gperftools/profiler.h>
#include <gflags/gflags.h>

#include "base/logging.h"
#include "base/walltime.h"
#include "strings/human_readable.h"
#include "strings/numbers.h"
#include "strings/split.h"
#include "strings/strcat.h"
#include "util/http/http_server.h"

using std::string;
using std::vector;
using strings::AsString;

namespace http {
namespace {
char last_profile_suffix[100] = {0};
}

namespace internal {

static void HandleCpuProfile(bool enable, Response* response) {
  string profile_name = "/tmp/" + base::ProgramBaseName();
  if (enable) {
    if (last_profile_suffix[0]) {
      response->AppendContent("<p> Yo, already profiling, stupid!</p>\n");
    } else {
      string suffix = base::LocalTimeNow("_%d%m%Y_%H%M%S.prof");
      profile_name.append(suffix);
      strcpy(last_profile_suffix, suffix.c_str());
      int res = ProfilerStart(profile_name.c_str());
      LOG(INFO) << "Starting profiling into " << profile_name << " " << res;
      response->AppendContent("<p> Yeah, let's profile this bitch, baby!</p> \n"
        "<img src='//super3s.com/files/2012/12/weasel_with_hula_hoop_hc-23g0lmj.gif'>\n");
    }
    return;
  }
  ProfilerStop();
  if (last_profile_suffix[0] == '\0') {
    response->AppendContent("<h3>Profiling is off, commander!</h3> \n");
    return;
  }
  string cmd("nice -n 15 pprof --svg ");
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

  // TODO(roman): to use more efficient implementation of popen from
  //  https://github.com/famzah/popen-noshell
  FILE* pipe = popen(cmd.c_str(), "r");
  if (pipe == nullptr) {
    LOG(ERROR) << "Error running it, status: " << errno << " " << strerror(errno);
    response->Send(HTTP_INTERNAL_SERVER_ERROR);
    return;
  }
  char buf[200];
  while (fgets(buf, sizeof(buf), pipe)) {
    LOG(INFO) << StringPiece(buf, sizeof(buf));
  }
  pclose(pipe);

  // Redirect browser to show this file.
  string url("/filez?file=");
  url.append(profile_name);
  LOG(INFO) << "Redirecting to " << url;

  response->AddHeader("Cache-Control", "no-cache, no-store, must-revalidate");
  response->AddHeader("Pragma", "no-cache");
  response->AddHeader("Expires", "0");
  response->AddHeaderCopy("Location", url.c_str());
  response->Send(HTTP_MOVED_PERMANENTLY);
}

static string HumanReadableBytes(StringPiece key, int64 val) {
  return StrCat(key, ": ", HumanReadableNumBytes::ToString(val), "\n");
}

void ProfilezHandler(const Request& request, Response* response) {
  VLOG(1) << "query: " << request.query();
  response->SetContentType(Response::kHtmlMime);
  Request::KeyValueArray args = request.ParsedQuery();
  bool pass = false;
  bool enable = false;
  std::unique_ptr<char[]> mem_stats;
  MallocExtension* mem_ext = MallocExtension::instance();

  for (const auto& k_v : args) {
    if (k_v.first == "tok" && k_v.second == "") {
      pass = true;
    } else if (k_v.first == "profile" && k_v.second == "on") {
      enable = true;
    } else if (k_v.first == "mem") {
      constexpr int kBufSize = 1024 * 10;
      mem_stats.reset(new char[kBufSize]);
      mem_ext->GetStats(mem_stats.get(), kBufSize);
    }
  }
  if (!pass) {
    response->Send(HTTP_UNAUTHORIZED);
    return;
  }
  response->AppendContent(R"(<!DOCTYPE html>
    <html>
      <head> <title>Profilez</title> </head>
      <body>)");
  if (mem_stats) {
    size_t max_thread_cache = 0, current_thread_cache = 0, free_thread_cache = 0;
    mem_ext->GetNumericProperty("tcmalloc.max_total_thread_cache_bytes", &max_thread_cache);
    mem_ext->GetNumericProperty("tcmalloc.current_total_thread_cache_bytes", &current_thread_cache);
    mem_ext->GetNumericProperty("tcmalloc.thread_cache_free_bytes", &free_thread_cache);
    response->AppendContent("<pre>")
      .AppendContent(HumanReadableBytes("max_total_thread_cache", max_thread_cache))
      .AppendContent(HumanReadableBytes("current_total_thread_cache", current_thread_cache))
      .AppendContent(HumanReadableBytes("free_thread_cache", free_thread_cache))
      .AppendContent(mem_stats.get()).AppendContent("</pre>");
  } else {
    HandleCpuProfile(enable, response);
  }
  response->AppendContent("</body> </html>\n");
  response->Send(HTTP_OK);
}

void FilezHandler(const Request& request, Response* response) {
  Request::KeyValueArray args = request.ParsedQuery();
  bool pass = false;
  StringPiece file_name;
  for (const auto& k_v : args) {
    if (k_v.first == "tok" && k_v.second == "") {
      pass = true;
    }
    if (k_v.first == "file") {
      file_name = k_v.second;
    }
  }
  if (!pass || file_name.empty()) {
    response->Send(HTTP_UNAUTHORIZED);
    return;
  }
  if (absl::EndsWith(file_name, ".svg")) {
    response->SetContentType("image/svg+xml");
  } else if (absl::EndsWith(file_name, ".html")) {
    response->SetContentType(Response::kHtmlMime);
  } else {
    response->SetContentType(Response::kTextMime);
  }
  response->SendFile(file_name.data(), HTTP_OK);
}

void FlagzHandler(const Request& request, Response* response) {
  Request::KeyValueArray args = request.ParsedQuery();
  bool pass = false;
  StringPiece flag_name;
  StringPiece value;
  for (const auto& k_v : args) {
    if (k_v.first == "tok" && k_v.second == "") {
      pass = true;
    }
    if (k_v.first == "flag") {
      flag_name = k_v.second;
    } else if (k_v.first == "value") {
      value = k_v.second;
    }
  }
  if (!pass) {
    response->Send(HTTP_UNAUTHORIZED);
    return;
  }
  if (!flag_name.empty()) {
    google::CommandLineFlagInfo flag_info;
    if (!google::GetCommandLineFlagInfo(flag_name.data(), &flag_info)) {
      response->AppendContent("Flag not found \n");
    } else {
      response->SetContentType(Response::kHtmlMime);
      response->AppendContent("<p>Current value ").AppendContent(flag_info.current_value)
          .AppendContent("</p>");
      string res = google::SetCommandLineOption(flag_name.data(), value.data());
      response->AppendContent("Flag ").AppendContent(res);
      if (flag_name == "vmodule") {
        vector<StringPiece> parts = absl::StrSplit(value, ",", absl::SkipEmpty());
        for (StringPiece p : parts) {
          size_t sep = p.find('=');
          int32 level = 0;
          if (sep != StringPiece::npos && safe_strto32(p.substr(sep + 1), &level)) {
            string module_expr = AsString(p.substr(0, sep));
            int prev = google::SetVLOGLevel(module_expr.c_str(), level);
            LOG(INFO) << "Setting module " << module_expr << " to loglevel " << level
                      << ", prev: " << prev;
          }
        }
      }
    }
  } else if (args.size() == 1) {
    LOG(INFO) << "Printing all flags";
    std::vector<google::CommandLineFlagInfo> flags;
    google::GetAllFlags(&flags);
    for (const auto& v : flags) {
      response->AppendContent("--").AppendContent(v.name).AppendContent(": ").
          AppendContent(v.current_value).AppendContent("\n");
    }
  }

  response->Send(HTTP_OK);
}

}  // namespace internal
}  // namespace http
