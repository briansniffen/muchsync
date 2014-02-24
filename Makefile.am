
AM_CPPFLAGS = $(sqlite3_CFLAGS) $(libcrypto_CFLAGS) $(xapian_CPPFLAGS)
LDADD = $(sqlite3_LIBS)	$(libcrypto_LIBS) $(xapian_LIBS) -lnotmuch

bin_PROGRAMS = muchsync

muchsync_SOURCES = hash.cc infinibuf.cc muchsync.cc protocol.cc		\
	spawn.cc sqlstmt.cc xapian_sync.cc muchsync.h infinibuf.h	\
	sqlstmt.h

CLEANFILES = *~
maintainer-clean-local:
	+@echo rm -rf `sed -ne 's!^/!!p' .gitignore` Makefile.in
	rm -rf `sed -ne 's!^/!!p' .gitignore` Makefile.in
