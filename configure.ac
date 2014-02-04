#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.69])
AC_INIT(muchsync, 0)
AM_INIT_AUTOMAKE([-Wall])
AC_CONFIG_SRCDIR([configure.ac])
AC_CONFIG_MACRO_DIR([m4])

AC_PROG_CXX
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
   || ! xapian_LIBS=$($XAPIAN_CONFIG --ltlibs); then
   AC_MSG_ERROR(Error running $XAPIAN_CONFIG)
fi
AC_SUBST(xapian_CPPFLAGS)
AC_SUBST(xapian_LIBS)

AC_CONFIG_FILES([Makefile])
AC_OUTPUT

