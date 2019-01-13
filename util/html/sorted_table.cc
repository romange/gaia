// Copyright 2013, Ubimo.com .  All rights reserved.
// Author: Roman Gershman (roman@ubimo.com)
//
#include "util/html/sorted_table.h"

namespace util {
namespace html {
using std::string;

void SortedTable::StartTable(const std::vector<StringPiece>& header, string* dest) {
}

string SortedTable::HtmlStart() {
  return R"(
html>

<head>
  <!-- <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/jquery.tablesorter/2.31.1/css/theme.bootstrap.min.css">
  <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/jquery.tablesorter/2.31.1/css/theme.bootstrap_4.min.css"> -->
  <link rel="stylesheet" href="//cdnjs.cloudflare.com/ajax/libs/jquery.tablesorter/2.31.1/css/theme.blue.min.css">
  <link rel="stylesheet" href="//cdnjs.cloudflare.com/ajax/libs/jquery.tablesorter/2.31.1/css/jquery.tablesorter.pager.min.css">
  <link rel="stylesheet" href="style.css">
  <script src="//code.jquery.com/jquery-latest.min.js"></script>
  <script src="//cdnjs.cloudflare.com/ajax/libs/jquery.tablesorter/2.31.1/js/jquery.tablesorter.min.js"></script>
  <script src="//cdnjs.cloudflare.com/ajax/libs/jquery.tablesorter/2.31.1/js/jquery.tablesorter.widgets.min.js"></script>
  <script src="//cdnjs.cloudflare.com/ajax/libs/jquery.tablesorter/2.31.1/js/extras/jquery.tablesorter.pager.min.js"></script>
  <script src="main.js"></script>
</head>
)";
}

void SortedTable::EndTable(std::string* dest) {
  dest->append("</tbody></table>\n");
}

}  // namespace html
}  // namespace util
