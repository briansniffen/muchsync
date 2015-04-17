% muchsync - synchronize notmuch mail state across machines

[Notmuch](http://notmuchmail.org/) is a nice mail indexer with front
ends for [emacs](https://www.gnu.org/software/emacs/) and
[vim](http://www.vim.org/).  If you like the idea of fully-indexed,
tag-based email like gmail, but you don't want a cloud- or web-based
solution, then notmuch may be for you.  However, notmuch stores all of
your mail locally on one machine.  Hence, until now, if you wanted the
full benefit of notmuch tags, you could only conveniently read your
email on a single machine.

Muchsync brings notmuch to all of your computers by synchronizing your
mail messages and notmuch tags across machines.  The protocol is
heavily pipelined to work efficiently over high-latency networks such
as mobile networks.  Muchsync supports pairwise synchronization among
arbitrary many replicas.  A version-vector-based algorithm allows it
to exchange only the minimum information necessary to bring replicas
up to date regardless of which pairs have previously synchronized.

Setting up muchsync is as easy as typing this the first time you run
it:

    muchsync --init $HOME/inbox SERVER

Here `SERVER` is your existing mail server.  Initialization can take a
*very* long time because it downloads all your email and builds a full
text index on a single CPU.  This is a limitation of how notmuch
tracks threads, which makes it impossible to parallelize first-time
index creation.

Once setup, using muchsync is as easy as typing this each time you
want to check for new mail:

    muchsync SERVER

That command brings bring the client and `SERVER` up to date with any
tag and message changes, and should generally run efficiently if you
don't have much mail to download.  If, after using the above command
to synchronize your desktop with a server, you also want your mail on
a laptop, you can push the mail from your desktop to the laptop with:

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
