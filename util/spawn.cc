// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include <errno.h>
#include <spawn.h>
#include <sys/wait.h>
#include <unistd.h>

#include <string>

#include "util/spawn.h"

extern "C" char **environ;

namespace util {

// Runs a child process efficiently.
// argv must be null terminated array of arguments where the first item in the array is the base
// name of the executable passed by path.
// Returns 0 if child process run successfully and updates child_status with its return status.
static int spawn(const char* path, char* argv[], int* child_status) {
  pid_t pid;
  // posix_spawn in this configuration calls vfork without duplicating parents
  // virtual memory for the child. That's all we want.
  int status = posix_spawn(&pid, path, NULL, NULL, argv, environ);
  if (status != 0)
    return status;

  if (waitpid(pid, child_status, 0) == -1)
    return errno;
  return 0;
}

int sh_exec(const char* cmd) {
  char sh_bin[] = "sh";
  char arg1[] = "-c";

  std::string cmd2(cmd);

  char* argv[] = {sh_bin, arg1, &cmd2.front(), NULL};
  int child_status = 0;
  return spawn("/bin/sh", argv, &child_status);
}

}  // namespace util
