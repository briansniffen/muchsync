
CC=cc
CFLAGS=-Wall -g -O

notmuch_CPPFLAGS=
notmuch_LDADD=-lnotmuch

PKGS=libcrypto sqlite3 talloc

CPPFLAGS=$(notmuch_CPPFLAGS) `pkg-config --cflags $(PKGS)`
LDADD=$(notmuch_LDADD) `pkg-config --libs $(PKGS)`

all: muchsync
.PHONY: all

muchsync: muchsync.c
	$(CC) $(CFLAGS) -o $@ muchsync.c $(LDADD)

clean:
	rm -f muchsync *.o *~
.PHONY: clean
