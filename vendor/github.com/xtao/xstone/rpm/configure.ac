#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.69])
AC_INIT(m4_esyscmd(grep Name META | cut -d ':' -f 2 | tr -d ' \n'),
	m4_esyscmd(grep Version META | cut -d ':' -f 2 | tr -d ' \n'))
AC_LANG(C)
AC_CANONICAL_SYSTEM
AM_MAINTAINER_MODE
AM_INIT_AUTOMAKE

# Checks for programs.
AC_PROG_AWK
#AC_PROG_GO

# Checks for libraries.

# Checks for header files.

# Checks for typedefs, structures, and compiler characteristics.

# Checks for library functions.

AC_CONFIG_FILES([
Makefile
xstone/Makefile
])
AC_OUTPUT
