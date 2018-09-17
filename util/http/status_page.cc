// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/http/status_page.h"

#include "absl/strings/str_replace.h"
#include "base/walltime.h"
#include "util/proc_stats.h"
#include "util/stats/varz_stats.h"

namespace util {
namespace http {
using namespace std;
using namespace boost;
using beast::http::field;

namespace {

string GetTimerString(uint64 seconds) {
  char buf[128];
  uint32 hours = seconds / 3600;
  seconds = seconds % 3600;
  uint32 mins = seconds / 60;
  uint32 secs = seconds % 60;
  snprintf(buf, sizeof buf, "%" PRIu32 ":%" PRIu32 ":%" PRIu32, hours, mins, secs);
  return buf;
}

string StatusLine(const string& name, const string& val) {
  string res("<div>");
  res.append(name).append(":<span class='key_text'>").append(val).append("</span></div>\n");
  return res;
}
}  // namespace

void BuildStatusPage(const QueryArgs& args, const char* resource_prefix,
                     HttpHandler::Response* response) {
  bool output_json = false;

  string varz;
  VarzListNode::IterateValues([&varz](const string& nm, const string& val) {
    absl::StrAppend(&varz, "\"", nm, "\": ", val, ",\n");
  });
  if (varz.size() > 1) {
    varz.resize(varz.size() - 2);
  }

  for (const auto& k_v : args) {
    if (k_v.first == "o" && k_v.second == "json")
      output_json = true;
  }

  if (output_json) {
    response->set(field::content_type, kJsonMime);
    response->body() = "{" + varz + "}\n";
    return;
  }

  string a = "<!DOCTYPE html>\n<html><head>\n";
  a += R"(<meta http-equiv='Content-Type' content='text/html; charset=UTF-8'/>
    <link href='http://fonts.googleapis.com/css?family=Roboto:400,300' rel='stylesheet'
     type='text/css'>
    <link rel='stylesheet' href='{s3_path}/status_page.css'>
    <script type="text/javascript" src="{s3_path}/status_page.js"></script>
</head>
<body>
<div><img src='{s3_path}/logo.png'/></div>)";

  a = absl::StrReplaceAll(a, {{"{s3_path}", resource_prefix}});

  a += "\n<div class='left_panel'></div>\n";
  a += "<div class='styled_border'>\n";
  a += StatusLine("Status", "OK");

  util::ProcessStats stats = util::ProcessStats::Read();
  time_t now = time(NULL);
  a += StatusLine("Started on", base::PrintLocalTime(stats.start_time_seconds));
  a += StatusLine("Uptime", GetTimerString(now - stats.start_time_seconds));

  a += R"(</div>
</body>
<script>
var json_text1 = {)";
  a += varz + R"(};
document.querySelector('.left_panel').innerHTML = JsonToHTML(json_text1);
</script>
</html>)";

  response->set(field::content_type, kHtmlMime);
  response->body() = std::move(a);
}

}  // namespace http
}  // namespace util
