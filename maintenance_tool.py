import os
import shutil

import pymongo


def purge_all():
    # clean database
    mongo_clt = pymongo.MongoClient()
    mongo_clt.drop_database('qz_crawler')
    # clean file
    for name in os.listdir():
        if name.isnumeric():
            shutil.rmtree(name)


def purge_by_uin(uin):
    # clean database
    mongo_clt = pymongo.MongoClient()
    mongo_db = mongo_clt['qz_crawler']
    mongo_db.drop_collection(uin)
    # clean file
    shutil.rmtree(uin)
