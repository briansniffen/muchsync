
AM_CPPFLAGS = $(sqlite3_CFLAGS) $(libcrypto_CFLAGS) $(xapian_CPPFLAGS) \
	$(talloc_CPPFLAGS)
LDADD = $(sqlite3_LIBS) $(libcrypto_LIBS) $(xapian_LIBS) \
	$(talloc_LIBS) -lnotmuch

bin_PROGRAMS = muchsync

muchsync_SOURCES = muchsync.cc notmuch.cc sqlite.cc muchsync.h

CLEANFILES = *~
maintainer-clean-local:
	+@echo rm -rf `sed -ne 's!^/!!p' .gitignore` Makefile.in
	rm -rf `sed -ne 's!^/!!p' .gitignore` Makefile.in
