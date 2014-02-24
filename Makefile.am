
AM_CPPFLAGS = $(BOOST_CPPFLAGS) $(sqlite3_CFLAGS) $(libcrypto_CFLAGS)	\
	$(xapian_CPPFLAGS)
LDADD = $(BOOST_LDFLAGS) $(BOOST_IOSTREAMS_LIB) $(sqlite3_LIBS)	\
	$(libcrypto_LIBS) $(xapian_LIBS) -lnotmuch

bin_PROGRAMS = muchsync

muchsync_SOURCES = hash.cc infinibuf.cc muchsync.cc protocol.cc	\
	spawn.cc sqlstmt.cc work_queue.cc xapian_sync.cc chan.h	\
	muchsync.h infinibuf.h sqlstmt.h work_queue.h

CLEANFILES = *~
maintainer-clean-local:
	+@echo rm -rf `sed -ne 's!^/!!p' .gitignore` Makefile.in
	rm -rf `sed -ne 's!^/!!p' .gitignore` Makefile.in
