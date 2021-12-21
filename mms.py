import os, pprint, sys, subprocess
import pymongo


class MongoException(Exception):
    pass


def ls():
    client = pymongo.MongoClient()
    ret = client.list_database_names()
    client.close()
    return sorted(ret)


def ls_db(db):
    if db not in ls():
        raise MongoException("db '{}' does not exist".format(db))
    client = pymongo.MongoClient()
    db = client[db]
    ret = db.list_collection_names()
    client.close()
    return sorted(ret)


def cp_db(src, dst, force=False):
    ls_ = ls()
    if src not in ls_:
        raise MongoException("source '{}' does not exist".format(src))
    if src == dst:
        return
    if dst in ls_:
        if not force:
            raise MongoException("destination '{}' exists".format(dst))
        else:
            rm_db(dst)
    cmd = "mongodump --archive --db={} --quiet | mongorestore --archive  --nsFrom={}.* --nsTo={}.* --quiet".format(
        src, src, dst
    )
    os.system(cmd)


def rm_db(db, force=False):
    if db not in ls():
        if not force:
            raise MongoException("db '{}' does not exist".format(db))
        else:
            return
    client = pymongo.MongoClient()
    client.drop_database(db)
    client.close()


def mv_db(src, dst, force=False):
    if src == dst:
        if src not in ls_:
            raise MongoException("source '{}' does not exist".format(src))
        return
    cp_db(src, dst, force=force)
    rm_db(src)


def cp_collection(db, src, dst, force=False):
    if db not in ls():
        raise MongoException("db '{}' does not exist".format(db))
    ls_ = ls_db(db)
    if src not in ls_:
        raise MongoException("source '{}.{}' does not exist".format(db, src))
    if src == dst:
        return
    if dst in ls_:
        if not force:
            raise MongoException("destination '{}.{}' exists".format(db, dst))
        else:
            rm_collection(db, dst)
    cmd = (
        "mongodump --archive --db={} --collection={} --quiet"
        "|"
        "mongorestore --archive  --nsFrom={}.{} --nsTo={}.{} --quiet"
    ).format(db, src, db, src, db, dst)
    os.system(cmd)


def rm_collection(db, col, force=False):
    if db not in ls():
        if not force:
            raise MongoException("db '{}' does not exist".format(db))
        else:
            return
    if col not in ls_db(db):
        if not force:
            raise MongoException("collection '{}.{}' does not exist".format(db, col))
        else:
            return
    client = pymongo.MongoClient()
    client[db][col].drop()
    client.close()


def mv_collection(db, src, dst, force=False):
    if src == dst:
        if db not in ls():
            raise MongoException("db '{}' does not exist".format(db))
        ls_ = ls_db(db)
        if src not in ls_:
            raise MongoException("source '{}.{}' does not exist".format(db, src))
        return
    cp_collection(db, src, dst, force=force)
    rm_collection(db, src)


def cat_collection(db, col, stream=sys.stdout):
    if db not in ls():
        raise MongoException("db '{}' does not exist".format(db))
    if col not in ls_db(db):
        raise MongoException("collection '{}.{}' does not exist".format(db, col))
    client = pymongo.MongoClient()
    ret = client[db][col].find({})
    pp = pprint.PrettyPrinter(indent=4, stream=stream)
    for doc in ret:
        try:
            pp.pprint(doc)
        except BrokenPipeError as e:
            break
    ret.close()
    client.close()


def less_collection(db, col):
    if db not in ls():
        raise MongoException("db '{}' does not exist".format(db))
    if col not in ls_db(db):
        raise MongoException("collection '{}.{}' does not exist".format(db, col))
    try:
        p = subprocess.Popen(
            ["less"],
            universal_newlines=True,
            stdin=subprocess.PIPE,
        )
        cat_collection(db, col, stream=p.stdin)
    finally:
        p.communicate()
