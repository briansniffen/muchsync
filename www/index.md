% muchsync

<div class="text-center" style="margin-top:10px;">
<img src="logo.png"/ alt="logo" width="524" height="119"
style="max-width:100%;"/>
</div>

# muchsync - synchronize [notmuch mail](http://notmuchmail.org/) across machines

[Notmuch](http://notmuchmail.org/) is a nice mail indexer with front
ends for emacs and vim.  If you like the idea of fully-indexed,
tag-based email like gmail, but you don't want a cloud- or web-based
solution, then notmuch may be for you.  However, notmuch stores all of
your mail locally on one machine.  Hence, until now, if you wanted the
full benefit of notmuch tags, you could only conveniently read your
email on a single machine.

Muchsync brings notmuch to all of your computers by synchronizing your
mail messages and notmuch tags across machines.  The protocol is
heavily pipelined to work efficiently over high-latency networks such
as mobile broadband.  Muchsync supports arbitrary pairwise
synchronization among replicas.  A version-vector-based algorithm
allows it to exchange only the minimum information necessary to bring
replicas up to date regardless of which pairs have previously
synchronized.  Muchsync requires storage proportional to the number of
replicas plus the number of email messages you have.  It consumes
bandwidth proportional to the number of replicas plus the number of
new/changed messages.  In practice, the per-replica data is tiny (just
two 64-bit numbers per replica, independent of how many messages you
have), so there is no penalty for having many replicas.

To set up muchsync, you must install it on both your mail server and
each machine that will replicate your mail.  To create a new replica,
you need to give muchsync two pieces of information: 1) the name of a
remote machine currently storing your email, to which you must have
ssh access, and 2) the name of a local directory muchsync can create
to store a copy of your mail.  If the remote machine is `SERVER` and
the destination directory is `$HOME/inbox`, you would run:

    muchsync --init $HOME/inbox SERVER

Initialization will create a `$HOME/.notmuch-config` file for you
based on the one on `SERVER`.  Note that if you've run muchsync
before, `SERVER` can be any replica; it doesn't have to be your main
mail server.  This command will download and index all of your mail.
Initialization can take a *very* long time because it builds a full
text index on a single CPU.  This is a limitation of how notmuch
tracks threads, which makes it impossible to parallelize first-time
index creation.

Once set up, using muchsync is as easy as typing this each time you
want to check for new mail:

    muchsync SERVER

That command brings the client and `SERVER` up to date with any tag
and message changes, and should generally run efficiently if you don't
have much mail to download.  If, after using the above command to
synchronize your desktop with a server, you also want your mail on a
laptop, you can push the mail from your desktop to the laptop with:

    muchsync LAPTOP

## Requirements

To build muchsync, you will need a C++11 compiler and the headers and
libraries from [notmuch](http://www.notmuchmail.org/),
[Xapian](http://xapian.org), [SQLite3](http://www.sqlite.org/), and
[OpenSSL](https://www.openssl.org)'s libcrypto.  To run muchsync, you
will need [ssh](http://www.openssh.com/).

## Download

* [Click here to download releases](src/)

* Or, to contribute, clone the repo with:

    ~~~~
    git clone http://www.muchsync.org/muchsync.git
    ~~~~

## Documentation

* [muchsync(1)](muchsync.html)
* [notmuch manual pages](http://notmuchmail.org/manpages/)

## Contact

Please [email](http://www.scs.stanford.edu/~dm/addr/) questions,
comments, testimonials, bug reports, patches, and pull requests to the
author (preferably with the word "muchsync" in the email subject).
For pull requests, please spare me the trouble of navigating any web
sites and include a raw `git fetch` command that makes `FETCH_HEAD`
point to whatever it is you would like me to merge.

muchsync is brought to you by
[David Mazi&egrave;res](http://www.scs.stanford.edu/~dm/) of the
[Stanford Secure Computer Systems group](http://www.scs.stanford.edu/)
and [Mail Avenger](http://www.mailavenger.org/) project.
