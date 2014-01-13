
AM_CPPFLAGS = $(BOOST_CPPFLAGS) $(sqlite3_CFLAGS) $(libcrypto_CFLAGS)	\
	$(xapian_CPPFLAGS)
LDADD = $(BOOST_LDFLAGS) $(BOOST_IOSTREAMS_LIB) $(sqlite3_LIBS)	\
	$(libcrypto_LIBS) $(xapian_LIBS) -lnotmuch

bin_PROGRAMS = muchsync

muchsync_SOURCES = fdstream.cc maildir.cc muchsync.cc protocol.cc	\
	sqlite.cc xapian_sync.cc muchsync.h fdstream.h

CLEANFILES = *~
maintainer-clean-local:
	+@echo rm -rf `sed -ne 's!^/!!p' .gitignore` Makefile.in
	rm -rf `sed -ne 's!^/!!p' .gitignore` Makefile.in
