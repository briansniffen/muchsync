% muchsync - synchronize maildirs and notmuch databases

[Notmuch](http://notmuchmail.org/) is a nice mail indexer with front
ends for [emacs](https://www.gnu.org/software/emacs/) and
[vim](http://www.vim.org/).  If you like the idea of fully-indexed,
tag-based email like gmail, but you don't want a cloud- or web-based
solution, then notmuch may be for you.  However, because notmuch
stores all of your mail locally, until now you could only conveniently
read mail on a single machine.

muchsync brings notmuch to all of your computers by synchronizing your
mail messages and notmuch tags across machines.  The protocol is
heavily pipelined to work efficiently over high-latency networks as
when tethering to a cell phone.  Finally, muchsync supports pairwise
synchronization among arbitrary many replicas.  A version-vector-based
algorithm allows it to exchange only the minimum information necessary
to bring replicas up to date regardless of which pairs have previously
synchronized.

Setting up muchsync is as easy as typing this the first time you run
it:

    muchsync --init $HOME/inbox SERVER

Here `SERVER` is your existing mail server.  Initialization can take a
very long time because it builds a mail index and notmuch currently
makes it impossible to spread that task over more than one CPU.

Using muchsync is as easy as typing this each time you want to check
for new mail:

    muchsync SERVER

That command brings bring the client and `SERVER` up to date with any
tag and message changes.  Of course, if after synchronizing with a
server you want to push the changes onto another machine `LAPTOP`,
then all you need to run is:

    muchsync LAPTOP

## Requirements

To build muchsync, you will need a C++11 compiler and the headers and
libraries from [notmuch](http://www.notmuchmail.org/),
[Xapian](http://xapian.org), [SQLite3](http://www.sqlite.org/), and
[OpenSSL](https://www.openssl.org)'s libcrypto.  To run muchsync, you
will need [ssh](http://www.openssh.com/).

## [Download](src/)

## Documentation

* [muchsync(1)](muchsync.html)
* [notmuch manual pages](http://notmuchmail.org/manpages/)

## Contact

Please [email](http://www.scs.stanford.edu/~dm/addr/) questions,
comments, testimonials, and bug reports to the author (preferably with
the word "muchsync" in the email subject).

muchsync is brought to you by
[David Mazi&egrave;res](http://www.scs.stanford.edu/~dm/) of the
[Stanford Secure Computer Systems group](http://www.scs.stanford.edu/)
and [Mail Avenger](http://www.mailavenger.org/) project.