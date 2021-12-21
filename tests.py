import os, pprint
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
    MongoException,
)


def unit_test():
    try:
        print("dbs:", ls())
        print("fishtest_tests:", ls_db("fishtest_tests"))
        rm_db("fishtest_tests2", force=True)
        cp_db("fishtest_tests", "fishtest_tests2")
        print("dbs:", ls())
        print("fishtest_test2:", ls_db("fishtest_tests2"))
        cp_collection("fishtest_tests2", "actions", "actions2")
        print("fishtest_test2:", ls_db("fishtest_tests2"))
        rm_collection("fishtest_tests2", "actions2")
        print("fishtest_test2:", ls_db("fishtest_tests2"))
        rm_db("fishtest_tests2")
        print("dbs:", ls())
    except MongoException as e:
        print("Exception:", e)


if __name__ == "__main__":
    unit_test()
