
CC=cc
CFLAGS=-Wall -g -O

notmuch_CPPFLAGS=
notmuch_LDADD=-lnotmuch

CPPFLAGS=$(notmuch_CPPFLAGS) `pkg-config --cflags sqlite3 talloc`
LDADD=$(notmuch_LDADD) `pkg-config --libs sqlite3 talloc`

all: muchsync
.PHONY: all

muchsync: muchsync.c
	$(CC) $(CFLAGS) -o $@ muchsync.c $(LDADD)

clean:
	rm -f muchsync *.o *~
.PHONY: clean
