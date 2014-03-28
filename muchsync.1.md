% muchsync(1)
% David Mazieres
% 

# NAME

inktool - synchronize maildirs and notmuch databases

# SYNOPSIS

muchsync _options_ \
muchsync _options_ _server-name_ _server-options_ \
muchsync _options_ --init _maildir_ _server-name_ _server-options_

# DESCRIPTION

muchsync synchronizes the contents of maildirs and notmuch tags across
machines.  Any given execution runs pairwise between two replicas, but
the system scales to an arbitrary number of replicas synchronizing in
arbitrary pairs.  For efficiency, version vectors and logical
timestamps are used to limit synchronization to items a peer may not
yet know about.

To use muchsync, both muchsync and notmuch should be installed
someplace in your PATH on two machines, and you must be able to access
the remote machine via ssh.

In its simplest usage, you have a single notmuch database on some
server `SERVER` and wish to start replicating that database on a
client, where the client currently does not have any mailboxes.  You
can initialize a new replica in `$HOME/inbox` by running the following
command:

    muchsync --init $HOME/inbox SERVER

This command may take some time, as it transfers the entire contents
of your maildir from the server to the client and creates a new
notmuch index on the client.  Depending on your setup, you may be
either bandwidth limited or CPU limited.  (Sadly, the notmuch library
on which muchsync is built is non-reentrant and forces all indexing to
happen on a single core at a rate of about 10,000 messages per
minute.)

From then on, to synchronize the client with the server, just run:

    muchsync SERVER

Since muchsync replicates the tags in the notmuch database itself, you
should consider disabling maildir flag synchronization by executing:

    notmuch config set maildir.synchronize_flags=false

The reason is that the synchronize\_flags feature only works on a
small subset of pre-defined flags and so is not all that useful.
Moreover, it marks flags by renaming files, which is not particularly
efficient.  muchsync was largely motivated by the need for better flag
synchronization.  If you are satisfied with the synchronize\_flags
feature, you might consider a tool such as offlineimap as an
alternative to muchsync.


## Synchronization algorithm

muchsync separately synchronizes two classes of information:  the
message-to-directory mapping (henceforth link counts) and the
message-id-to-tag mapping (henceforth tags).  Using logical
timestamps, it can detect update conflicts for each type of
information.  We describe link count and tag synchronization in turn.

Link count synchronization consists of ensuring that any given message
(identified by its collision-resistant content hash) appears the same
number of times in the same subdirectories on each replica.  Generally
a message will appear only once in a single subdirectory.  However, if
the message is moved or deleted on one replica, this will propagate to
other replicas.

If two replicas move or copy the same file between synchronization
events (or one moves the file and the other deletes it), this
constitutes an update conflict.  Update conflicts are resolved by
storing in each subdirectory a number of copies equal to the maximum
of the number of copies in that subdirectory on the two replicas.
This is conservative, in the sense that a file will never be deleted
after a conflict, though you may get extra copies of files.  (muchsync
uses hard links, so at least these copies will not use too much disk
space.)

For example, if one replica moves a message to subdirectory .box1/cur
and another moves the same message to subdirectory .box2/cur, the
conflict will be resolved by placing two links to the message on each
replica, one in .box1/cur and one in .box2/cur.  To respect the
structure of maildirs, subdirectories ending `new` and `cur` are
special-cased; conflicts between sibling `new` and `cur`
subdirectories are resolved in favor of `cur` without creating
additional copies of messages.

Message tags are synchronized based on notmuch's message-ID (usually
the Message-ID header of a message), rather than message contents.  On
conflict, tags are combined combined as follows.  Any tag in the
notmuch configuration parameter `new.tags` is removed from the message
unless it appears on both replicas.  Any other tag is added if it
appears on any replica.  In other words, tags in `new.tags` are
logically anded, while all other flags are logically ored.  (This
approach will give the most predictable results if `new.tags` has the
same value in all your replicas.  The `--init` option ensures this
initially, but subsequent changes to `new.tags` must be manually
propagated.)


# OPTIONS

\-C _file_, \--config _file_
:   Specify the path of the notmuch configuration file to use.  If
    none is specified, the default is to use the contents of the
    environment variable \$NOTMUCH_CONFIG, or if that variable is
    unset, the value \$HOME/.notmuch-config.  (These are the same
    defaults as the notmuch command itself.)

\-F
:   Check for modified files.  Without this option, muchsync assumes
    that files in a maildir are never edited.  -F disables certain
    optimizations so as to make muchsync at least check the timestamp
    on every file, which will detect modified files at the cost of a
    longer startup time.

\-r /path/to/muchsync
:   Specifies the path to muchsync on the server.  Ordinarily, muchsync
    should be in the default PATH on the server so this option is not
    required.  However, this option is useful if you have to install
    muchsync in a non-standard place or wish to test development
    versions of the code.

\-s ssh-cmd
:   Specifies a command line to pass to /bin/sh to execute a command on
    another machine.  The default value is "ssh -CTaxq".  Note that
    because this string is passed to the shell, special characters
    including spaces may need to be escaped.

\-v
:   The -v option increases verbosity.  The more times it is specified,
    the more verbose muchsync will become.

\--help
:   Print a brief summary of muchsync's command-line options.

\--init _maildir_
:   This option clones an existing mailbox on a remote server into
    _maildir_ on the local machine.  Neither _maildir_ nor your
    notmuch configuration file (see ```--config``` above) should exist
    when you run this command, as both will be created.  The
    configuration file is copied from the server (adjusted to reflect
    the local maildir), while _maildir_ is created as a replica of the
    maildir you have on the server.

\--nonew
:   Ordinarily, muchsync begins by running "notmuch new".  This option
    says not to run "notmuch new" before starting the muchsync
    operation.  It can be passed as either a client or a server
    option.  For example:  The command "```muchsync myserver
    --nonew```" will run "```notmuch new```" locally but not on
    myserver.

\--noup, \--noupload
:   Transfer files from the server to the client, but not vice versa.

\--upbg
:   Transfer files from the server to the client in the foreground.
    Then fork into the background to upload any new files from the
    client to the server.  This option is useful when checking new
    mail, if you want to begin reading your mail as soon as it has
    been downloaded while the upload continues.

\--version
:   Report on the muchsync version number

# EXAMPLES

To initialize a the muchsync database, you can run:

    muchsync -vv

This first executes "`notmuch new`", then builds the initial muchsync
database from the contents of your maildir (the directory specified as
`database.path` in your notmuch configuration file).  This command may
take several minutes the first time it is run, as it must compute a
content hash of every message in the database.  Note that you do not
need to run this command, as muchsync will initialize the database the
first time a client tries to synchronize anyway.

    muchsync --init ~/maildir myserver

First run "notmuch new" on myserver, then create a directory
`~/maildir` containing a replica of your mailbox on myserver.  Note
that neither your configuration file (by default `~/.notmuch-config`)
nor `~/maildir` should exist before running this command, as both will
be created.

To have the command ``notmuch new`` on a client automatically fetch
new mail from server `myserver`, you can place the following in the
file ``.notmuch/hooks/post-new`` under your mail directory:

    #!/bin/sh
    notmuch --nonew --upbg myserver

# FILES

The default notmuch configuration file is `$HOME/.notmuch-config`.

muchsync keeps all of its state in a subdirectory of your top maildir
called ```.notmuch/muchsync```.

# SEE ALSO

notmuch(1).

# BUGS

muchsync never deletes directories.  If you want to remove a
subdirectory completely, you must manually execute rmdir on all
replicas.  Even if you manually delete a subdirectory, it will live on
in the notmuch database.

To synchronize deletions and re-creations properly, muchsync never
deletes content hashes and their message IDs from its database, even
after the last copy of a message has disappeared.  Such stale hashes
should not consume an inordinate amount of disk space, but could
conceivably pose a privacy risk if users believe deleting a message
removes all traces of it.

Message tags are synchronized based on notmuch's message-ID (usually
the Message-ID header of a message), rather than based on message
contents.  This is slightly strange because very different messages
can have the same Message-ID header, meaning the user will likely only
read one of many messages bearing the same Message-ID header.  It is
conceivable that an attacker could suppress a message from a mailing
list by sending another message with the same Message-ID.  This bug is
in the design of notmuch, and hence not something that muchsync can
work around.  muchsync itself does not assume Message-ID equivalence,
relying instead on content hashes to synchronize link counts.  Hence,
any tools used to work around the problem should work on all replicas.

Because notmuch and Xapian do not keep any kind of modification time
on database entries, every invocation of muchsync requires a complete
scan of all tags in the Xapian database to detect any changed tags.
Fortunately muchsync heavily optimizes the scan so that it should take
well under a second for 100,000 mail messages.  However, this means
that interfaces such as those used by notmuch-dump are not efficient
enough (see the next paragraph).

muchsync makes certain assumptions about the structure of notmuch's
private types `notmuch_message_t` and `notmuch_directory_t`.  In
particular, it assumes that the Xapian document ID is the second field
of these the data structures.  Sadly, there is no efficient and clean
way to extract this information from the notmuch library interface.
muchsync also makes other assumptions about how tokens are named in
the Xapian database.  These assumptions are necessary because the
notmuch library interface and the notmuch dump utility are too slow to
support synchronization every time you check mail.

