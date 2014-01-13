
#include <cstring>
#include <iostream>
#include <iterator>
#include <fcntl.h>
#include <unistd.h>
#include <sys/wait.h>

#include "fdstream.h"
#include "muchsync.h"

using namespace std;

const string shell = "/bin/sh";

string
cmd_output (const string &cmd)
{
  int fds[2];
  if (pipe (fds))
    throw runtime_error (string ("pipe: ") + strerror (errno));
  fcntl (fds[0], F_SETFD, 1);

  pid_t pid = fork();
  switch (pid) {
  case -1:
    throw runtime_error (string ("fork: ") + strerror (errno));
    break;
  case 0:
    if (fds[1] != 1) {
      dup2 (fds[1], 1);
      close (fds[1]);
    }
    execl (shell.c_str(), shell.c_str(), "-c", cmd.c_str(), nullptr);
    cerr << shell << ": " << strerror (errno) << '\n';
    _exit (1);
    break;
  default:
    close (fds[1]);
    ifdstream stream{fds[0]};
    string ret{ istream_iterator<char>(stream), istream_iterator<char>() };
    waitpid (pid, NULL, 0);
    return ret;
  }
}
