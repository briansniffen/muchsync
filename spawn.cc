
#include <cstring>
#include <iostream>
#include <iterator>
#include <queue>
#include <fcntl.h>
#include <unistd.h>
#include <sys/wait.h>
#include <sys/socket.h>
#include <poll.h>

#include "muchsync.h"
#include "infinibuf.h"

using namespace std;

const string shell = "/bin/sh";

void
cmd_iofds (int fds[2], const string &cmd)
{
  int ifds[2], ofds[2];
  if (pipe (ifds))
    throw runtime_error (string ("pipe: ") + strerror (errno));
  if (pipe (ofds)) {
    close (ifds[0]);
    close (ifds[1]);
    throw runtime_error (string ("pipe: ") + strerror (errno));
  }

  pid_t pid = fork();
  switch (pid) {
  case -1:
    close (ifds[0]);
    close (ifds[1]);
    close (ofds[0]);
    close (ofds[1]);
    throw runtime_error (string ("fork: ") + strerror (errno));
    break;
  case 0:
    close (ifds[0]);
    close (ofds[1]);
    if (ofds[0] != 0) {
      dup2 (ofds[0], 0);
      close (ofds[0]);
    }
    if (ifds[1] != 1) {
      dup2 (ifds[1], 1);
      close (ifds[1]);
    }
    execl (shell.c_str(), shell.c_str(), "-c", cmd.c_str(), nullptr);
    cerr << shell << ": " << strerror (errno) << '\n';
    _exit (1);
    break;
  default:
    close (ifds[1]);
    close (ofds[0]);
    fcntl (ifds[0], F_SETFD, 1);
    fcntl (ofds[1], F_SETFD, 1);
    fds[0] = ifds[0];
    fds[1] = ofds[1];
    break;
  }
}

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
