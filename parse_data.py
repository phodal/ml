__author__ = 'fdhuang'
__all__ = ["get_minutes_counts_with_id"]

import json
import dateutil.parser
import sqlite3
import simplejson


def get_minutes_counts_with_id(jsonfile):
    datacount, dataarray = handle_json(jsonfile)
    minuteswithcount = [(x, dataarray.count(x)) for x in set(dataarray)]
    return minuteswithcount


def handle_json(jsonfile):
    f = open(jsonfile, "r")
    dataarray = []
    datacount = 0

    for line in open(jsonfile):
        line = f.readline()
        lin = json.loads(line)
        date = dateutil.parser.parse(lin["created_at"])
        datacount += 1
        dataarray.append(date.minute)

    f.close()
    return datacount, dataarray


def init_db(conn):
    c = conn.cursor()
    c.execute('''CREATE TABLE userinfo (owener text, language text, eventtype text, name text, url text)''')
    c.close()


def build_db(jsonfile):
    conn = sqlite3.connect('userdata.db')
    c = conn.cursor()
    # init_db(conn)
    f = open(jsonfile, "r")
    count = 1
    userinfo = []

    for line in open(jsonfile):
        date = f.readline()
        date = json.loads(date)
        if 'repository' in date:
            repo = date["repository"]
            if 'language' in repo:
                info = str(repo['owner']), str(repo['language']), str(date["type"]), str(repo["name"]), str(repo["url"])
                userinfo.append(info)
                count += 1

    c.executemany('INSERT INTO userinfo VALUES (?,?,?,?,?)', userinfo)
    f.close()
    conn.commit()
    c.close()


def build_all_db():
    for i in range(1, 20):
        if i < 10:
            filename = 'data/2014-02-0' + i.__str__() + '-0.json'
        else:
            filename = 'data/2014-02-' + i.__str__() + '-0.json'
        build_db(filename)


def get_minutes_count_num(jsonfile):
    datacount, dataarray = handle_json(jsonfile)
    return datacount


def get_month_total():
    monthdaycount = []
    for i in range(1, 20):
        if i < 10:
            filename = 'data/2014-02-0' + i.__str__() + '-0.json'
        else:
            filename = 'data/2014-02-' + i.__str__() + '-0.json'
        monthdaycount.append(get_minutes_count_num(filename))
    return monthdaycount