
#include <cstring>
#include <iostream>
#include <iterator>
#include <queue>
#include <fcntl.h>
#include <unistd.h>
#include <sys/wait.h>
#include <sys/socket.h>
#include <poll.h>

#include "fdstream.h"
#include "muchsync.h"

using namespace std;

const string shell = "/bin/sh";

struct buf {
  char *base_;
  char *first_;
  char *last_;
  char *lim_;

  explicit buf(size_t size = 0x10000)
    : base_(static_cast<char *> (malloc (size))), first_(base_), last_(base_),
      lim_(base_+size) {
    if (!base_)
      throw runtime_error (string ("buf::buf: cannot allocate memory"));
  }
  buf(const buf&) = delete;
  buf(buf &&b)
    : base_(b.base_), first_(b.first_), last_(b.last_), lim_(b.lim_) {
    b.base_ = b.first_ = b.last_ = b.lim_ = nullptr;
  }
  ~buf() { free (base_); }

  bool empty() { return first_ == last_; }
  bool full() { return last_ == lim_; }
  int input(int fd);
  void output(int fd);
};

int
buf::input(int fd)
{
  assert (!full());
  int n = read (fd, last_, lim_ - last_);
  if (n == 0)
    return 0;
  if (n < 0 && errno != EAGAIN)
    throw runtime_error (string("buf::input: ") + strerror (errno));
  last_ += n;
  return 1;
}

void
buf::output(int fd)
{
  assert (!empty());
  int n = write (fd, first_, last_ - first_);
  if (n >= 0)
    first_ += n;
  else if (errno != EAGAIN)
    throw runtime_error (string("buf::output: ") + strerror (errno));
}

void
make_nonblocking (int fd)
{
  int n;
  if ((n = fcntl (fd, F_GETFL)) < 0
      || fcntl (fd, F_SETFL, n | O_NONBLOCK) < 0)
    throw runtime_error (string ("O_NONBLOCK: ") + strerror (errno));
}

void
infinite_buffer (int infd, int outfd)
{
  struct pollfd fds[2];
  fds[0].fd = infd;
  fds[0].events = POLLRDNORM;
  fds[1].fd = outfd;
  fds[1].events = POLLWRNORM;
  bool eof = false;
  queue<buf> q;

  make_nonblocking (outfd);

  for (;;) {
    fds[0].revents = fds[1].revents = 0;
    while (!q.empty() && q.front().empty())
      q.pop();
    if (q.empty() && eof) {
      shutdown (outfd, 1);
      close (outfd);
      return;
    }
    poll (fds, q.empty() ? 1 : 2, -1);
    if (fds[0].revents) {
      if (q.empty() || q.back().full())
	q.push(buf());
      if (q.back().input (infd) <= 0) {
	fds[0].fd = -1;
	fds[0].events = 0;
	eof = true;
	if (infd != outfd)
	  close (infd);
      }
    }
    if (fds[1].revents)
      q.front().output(outfd);
  }
}

int
spawn_infinite_input_buffer (int infd)
{
  int fds[2];
  if (pipe (fds)) {
    cerr << string ("pipe: ") + strerror (errno);
    return -1;
  }

  pid_t pid = fork();
  switch (pid) {
  case -1:
    cerr << string ("fork: ") + strerror (errno);
    return -1;
  case 0:
    close (fds[0]);
    try { infinite_buffer (infd, fds[1]); }
    catch (...) { _exit (1); }
    _exit (0);
    break;
  default:
    close (fds[1]);
    return fds[0];
  } 
}

int
cmd_iofd (const string &cmd)
{
  int fds[2];
  if (socketpair(AF_UNIX, SOCK_STREAM, 0, fds) < 0)
    throw runtime_error (string ("socketpair: ") + strerror (errno));

  pid_t pid = fork();
  switch (pid) {
  case -1:
    throw runtime_error (string ("fork: ") + strerror (errno));
    break;
  case 0:
    close (fds[0]);
    if (fds[1] != 0)
      dup2 (fds[1], 0);
    if (fds[1] != 1)
      dup2 (fds[1], 1);
    if (fds[1] != 0 && fds[1] != 1)
      close (fds[1]);
    execl (shell.c_str(), shell.c_str(), "-c", cmd.c_str(), nullptr);
    cerr << shell << ": " << strerror (errno) << '\n';
    _exit (1);
    break;
  default:
    close (fds[1]);
    fcntl (fds[0], F_SETFD, 1);
    return fds[0];
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
