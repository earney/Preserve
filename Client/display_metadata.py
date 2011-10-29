#!/usr/bin/env python3

import sys

sys.path.append("../Common")

import DB

_db=DB.DB("/tmp/FS.db")

_db.execute("select * from directory_contents")
for _row in _db:
    print(_row)

print()
_db.execute("select * from attributes")
for _row in _db:
    print(_row)

del _db
