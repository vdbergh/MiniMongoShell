#!/usr/bin/env python3
import os, pprint, readline, atexit, os.path, sys
import pymongo
from mms import (
    ls,
    ls_db,
    cp_db,
    rm_db,
    mv_db,
    cp_collection,
    rm_collection,
    mv_collection,
    cat_collection,
    less_collection,
    indexes,
    MongoException,
)

banner = """
Welcome to MiniMongoShell.
Type 'help' for help.
"""
help = """
Commands
========

!<shell_command>
cd [<db>]
ls [<db>]
rm <db|collection>
cp <src> <dst>
mv <src> <dst>
cat <collection> [ > <file> ]
less <collection>
indexes <collection>
exit
"""


def repl():
    print(banner)
    cmd = ["cd", "ls", "rm", "cp", "mv", "cat", "less", "indexes", "exit"]
    state = "db"
    current_db = None
    readline.set_completer(lambda x, y: complete(x, y, cmd, ls()))
    while True:
        prompt = "{}> ".format(current_db) if state != "db" else "mms> "
        try:
            ans = input(prompt)
        except (EOFError, KeyboardInterrupt):
            print("")
            break
        if len(ans) == 0:
            continue
        ans_copy = ans
        if ans[0] == "!":
            if ans[1:].strip() == "":
                os.system("/bin/sh")
            else:
                os.system(ans[1:])
            continue
        ans = ans.split()
        if ans[0] == "help":
            if len(ans) != 1:
                print("Illegal command")
                continue
            print(help)
            continue
        if ans[0] == "exit":
            if len(ans) != 1:
                print("Illegal command")
                continue
            break
        if ans[0] == "ls":
            if len(ans) not in [1, 2]:
                print("Illegal command")
                continue
            if state == "db":
                if len(ans) == 1:
                    ret = ls(long=True)
                    for r in ret:
                        print("{:20s}{:10.0f}".format(r["name"], r["sizeOnDisk"]))
                    continue
                else:
                    ret = ls_db(ans[1], long=True)
            else:
                if len(ans) != 1:
                    print("Illegal command")
                    continue
                ret = ls_db(current_db, long=True)
            for r in ret:
                print("{:20s}{:10.0f}".format(r["name"], r["size"]))
            continue
        if ans[0] == "cd":
            if len(ans) not in [1, 2]:
                print("Illegal command")
                continue
            if len(ans) == 1:
                readline.set_completer(lambda x, y: complete(x, y, cmd, ls()))
                state = "db"
                continue
            elif ans[1] not in ls():
                print("'{}' does not exist".format(ans[1]))
                continue
            state = "col"
            current_db = ans[1]
            readline.set_completer(lambda x, y: complete(x, y, cmd, ls_db(current_db)))
            continue
        if ans[0] == "rm":
            if len(ans) != 2:
                print("Illegal command")
                continue
            if state == "db":
                try:
                    rm_db(ans[1])
                except MongoException as e:
                    print(e)
                continue
            else:
                try:
                    rm_collection(current_db, ans[1])
                except MongoException as e:
                    print(e)
                continue
        if ans[0] == "cat":
            if state == "db":
                print("Illegal command")
                continue
            else:
                stream = sys.stdout
                if ">" in ans_copy:
                    ans, file = ans_copy.split(">")
                    ans = ans.strip()
                    file = file.strip()
                    ans = ans.split()
                    if len(ans) != 2:
                        print("Illegal command")
                        continue
                    stream = open(file, "w")
                try:
                    cat_collection(current_db, ans[1], stream=stream)
                except MongoException as e:
                    print(e)
                finally:
                    if stream != sys.stdout:
                        stream.close()
                continue
        if ans[0] == "less":
            if state == "db":
                print("Illegal command")
                continue
            if len(ans) != 2:
                print("Illegal command")
                continue
            try:
                less_collection(current_db, ans[1])
            except MongoException as e:
                print(e)
            continue
        if ans[0] == "cp":
            if len(ans) != 3:
                print("Illegal command")
                continue
            if state == "db":
                try:
                    cp_db(ans[1], ans[2], force=True)
                except MongoException as e:
                    print(e)
                continue
            else:
                try:
                    cp_collection(current_db, ans[1], ans[2], force=True)
                except MongoException as e:
                    print(e)
                continue
        if ans[0] == "mv":
            if len(ans) != 3:
                print("Illegal command")
                continue
            if state == "db":
                try:
                    mv_db(ans[1], ans[2], force=True)
                except MongoException as e:
                    print(e)
                continue
            else:
                try:
                    mv_collection(current_db, ans[1], ans[2], force=True)
                except MongoException as e:
                    print(e)
                continue
        if ans[0] == "indexes":
            if len(ans) != 2:
                print("Illegal command")
                continue
            if state == "col":
                try:
                    for d in indexes(current_db, ans[1]):
                        #                        print("==============",d)
                        name_index = d["name"]
                        print("{:30s}".format(name_index), end="")
                        fields = d["key"]
                        for field, order in fields.items():
                            fields[field] = "asc" if order == 1 else "desc"
                        print("{:30s}".format(str(fields.to_dict())), end="")
                        filter = d.get("partialFilterExpression")
                        if filter is not None:
                            print("         {}".format(filter.to_dict()), end="")
                        print("")
                except MongoException as e:
                    print(e)
                continue
            else:
                print("Illegal command")
                continue

        print("Illegal command")


def save_history(prev_h_len, histfile):
    new_h_len = readline.get_current_history_length()
    readline.set_history_length(1000)
    readline.append_history_file(new_h_len - prev_h_len, histfile)


def complete(text, state, cmd, words):
    if " " not in readline.get_line_buffer():
        words = cmd
    results = [x + " " for x in words if x.startswith(text)] + [None]
    return results[state]


if __name__ == "__main__":
    readline.parse_and_bind("tab: complete")
    histfile = os.path.expanduser("~/.mongo_shell_history")
    try:
        readline.read_history_file(histfile)
        h_len = readline.get_current_history_length()
    except FileNotFoundError:
        open(histfile, "wb").close()
        h_len = 0
    atexit.register(save_history, h_len, histfile)
    repl()
