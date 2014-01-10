
#include <iostream>
#include <cstdio>

#include <boost/iostreams/device/file_descriptor.hpp>
#include <boost/iostreams/stream.hpp>

#include "muchsync.h"

using namespace std;
namespace io = boost::iostreams;

void
connect_to (const string &destination)
{
  string cmd;
  auto n = destination.find (':');
  if (n == string::npos)
    cmd = opt_ssh + " " + destination + " muchsync --server";
  else
    cmd = opt_ssh + " " + destination.substr(0, n) + " muchsync --server "
      + destination.substr(n);
}

static void
cmd_sync (sqlite3 *db, const versvector &vv)
{
  cout << "200 You asked for " << show_sync_vector(vv) << '\n';
}

void
muchsync_server (sqlite3 *db, const string &maildir)
{
  cout << "200 " << dbvers << '\n';
  string cmd;
  while ((cin >> cmd).good()) {
    if (cmd == "quit") {
      cout << "200 goodbye\n";
      return;
    }
    else if (cmd == "vect") {
      cout << "200 " << show_sync_vector (get_sync_vector (db)) << '\n';
    }
    else if (cmd == "sync") {
      versvector vv;
      string tail;
      if (!getline(cin, tail) || !read_sync_vector(tail, vv))
	cout << "500 could not parse vector\n";
      else
	cmd_sync (db, vv);
      continue;
    }
    else
      cout << "500 unknown verb " << cmd << '\n';
    cin.ignore (numeric_limits<streamsize>::max(), '\n');
  }
}

