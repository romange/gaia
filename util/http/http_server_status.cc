// Copyright 2014, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "base/walltime.h"
#include "strings/util.h"
#include "util/http/http_server.h"
#include "util/http/varz_stats.h"
#include "util/proc_stats.h"
#include "strings/strcat.h"

namespace http {

using std::string;

static const char kStaticFilesPrefix[] = "";

static string GetTimerString(uint64 seconds) {
  char buf[128];
  uint32 hours = seconds / 3600;
  seconds = seconds % 3600;
  uint32 mins = seconds /60;
  uint32 secs = seconds % 60;
  snprintf(buf, sizeof buf, "%" PRIu32 ":%" PRIu32 ":%" PRIu32, hours, mins, secs);
  return buf;
}

static string StatusLine(const string& name, const string& val) {
  string res("<div>");
  res.append(name).append(":<span class='key_text'>").append(val).append("</span></div>\n");
  return res;
}

std::string BuildStatusPage() {
  string a = "<!DOCTYPE html>\n<html><head>\n";
  a += "<meta http-equiv='Content-Type' content='text/html; charset=UTF-8' />\n"
       "<link href='http://fonts.googleapis.com/css?family=Roboto:400,300' rel='stylesheet' "
       "type='text/css'>\n";
  a += "<link rel='stylesheet' href='{s3_path}/status_page.css'>\n</head><body>\n";
  a += "<div><img src='{s3_path}/logo.png'/></div>\n";
  a += "<div class='left_panel'>";
  VarzListNode::IterateValues([&a](const string& nm, const string& val) {
    a += "<div style='margin-top:20px;'><span class='title_text'>";
    a.append(nm).append("</span>\n");
    a.append(val).append("</div>\n");
    a.append("<div class='separator'></div>\n");
  }, false);

  a += "</div>\n";
  strings::GlobalReplaceSubstring("{s3_path}", kStaticFilesPrefix, &a);

  a += "<div class='styled_border'>\n";
  a += StatusLine("Status", "OK");

  util::ProcessStats stats = util::ProcessStats::Read();
  time_t now = time(NULL);
  a += StatusLine("Started on", base::PrintLocalTime(stats.start_time_seconds));
  a += StatusLine("Uptime", GetTimerString(now - stats.start_time_seconds));
  a += "</div></body></html>\n";

  return a;
}

}  // namespace http
