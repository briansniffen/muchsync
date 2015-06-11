#!/bin/sh
if test ! -f muchsync.1 -a -z "$(command -v pandoc)"; then
    curl -o muchsync.1 http://www.muchsync.org/muchsync.1
fi
exec autoreconf -i
