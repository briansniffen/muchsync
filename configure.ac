#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.69])
AC_INIT(muchsync, 0)
AM_INIT_AUTOMAKE([-Wall])
AC_CONFIG_SRCDIR([configure.ac])
AC_CONFIG_MACRO_DIR([m4])

AC_PROG_CXX([clang++ eg++ g++ c++ gpp aCC CC cxx cc++ cl.exe FCC KCC RCC xlC_r xlC])
AX_CXX_COMPILE_STDCXX_11(noext,mandatory)
CXXFLAGS="$CXXFLAGS -Wall"
dnl AC_USE_SYSTEM_EXTENSIONS
AC_LANG(C++)
# -pthread Seems to be required by g++ -stc=c++11
AX_APPEND_COMPILE_FLAGS([-pthread])

PKG_CHECK_MODULES([sqlite3], [sqlite3])
PKG_CHECK_MODULES([libcrypto], [libcrypto])

AC_PATH_PROG(XAPIAN_CONFIG, xapian-config)
test -n "$XAPIAN_CONFIG" || AC_MSG_ERROR(Cannot find xapian-config)
if ! xapian_CPPFLAGS=$($XAPIAN_CONFIG --cxxflags) \
   || ! xapian_LIBS=$($XAPIAN_CONFIG --libs); then
   AC_MSG_ERROR(Error running $XAPIAN_CONFIG)
fi
AC_SUBST(xapian_CPPFLAGS)
AC_SUBST(xapian_LIBS)

AC_MSG_CHECKING(For st_mtim in struct stat)
AC_COMPILE_IFELSE(
[AC_LANG_PROGRAM([#include <time.h>
#include <sys/time.h>
#include <sys/stat.h>],
[int sz = sizeof(stat::st_mtim);])],
have_st_mtim=yes, have_st_mtim=no)
AC_MSG_RESULT($have_st_mtim)

if test yes = "$have_st_mtim"; then
   ST_MTIM=st_mtim
else
   AC_MSG_CHECKING(For st_mtimespec in struct stat)
   AC_COMPILE_IFELSE(
   [AC_LANG_PROGRAM([#include <time.h>
#include <sys/time.h>
#include <sys/stat.h>],
   [int sz = sizeof(stat::st_mtimespec);])],
   have_st_mtimespec=yes, have_st_mtimespec=no)
   AC_MSG_RESULT($have_st_mtimespec)
   if test yes = "$have_st_mtimespec"; then
      ST_MTIM=st_mtimespec
   else
	AC_MSG_ERROR(Cannot find nanoseconds mtime in stat struct)
   fi
fi
AC_DEFINE_UNQUOTED(ST_MTIM, $ST_MTIM,
Name of timespec modification time field in stat structure)

AC_CONFIG_FILES([Makefile])
AC_OUTPUT

