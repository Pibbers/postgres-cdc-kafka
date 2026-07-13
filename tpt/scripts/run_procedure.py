"""
Run a CREATE/REPLACE PROCEDURE (or any other single multi-statement DDL object)
against Teradata via the teradatasql driver instead of BTEQ.

BTEQ splits a script into requests on every top-level ';', which breaks stored
procedure bodies (they contain many internal ';'s inside BEGIN...END). Sending
the whole file as one teradatasql execute() call avoids that entirely.

Usage: python run_procedure.py <path-to-sql-file>
Requires TD_HOST / TD_USER / TD_PASSWORD in the environment. ${VAR} placeholders
in the file are substituted from the environment, same convention as run_bteq.sh.
"""
import os
import re
import sys

import teradatasql


def substitute(text: str) -> str:
    def repl(m):
        name = m.group(1)
        return os.environ.get(name, f"(UNDEF:{name})")
    return re.sub(r"\$\{([^}]+)\}", repl, text)


def main():
    path = sys.argv[1]
    with open(path, "r") as f:
        sql = f.read()
    sql = substitute(sql)

    con = teradatasql.connect(
        host=os.environ["TD_HOST"],
        user=os.environ["TD_USER"],
        password=os.environ["TD_PASSWORD"],
    )
    cur = con.cursor()
    cur.execute(sql)
    print(f"OK: executed {path}")
    con.close()


if __name__ == "__main__":
    main()
