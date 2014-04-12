import json
import sqlite3
import os
import glob
import gzip
import logging
from datetime import date, timedelta
import re


def init_db():
    conn = sqlite3.connect('userdata.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE userinfo (owner text, language text, eventtype text, name text, url text)''')
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


def handle_gzip_file(filename):
    userinfo = []
    with gzip.GzipFile(filename) as f:
        events = [line.decode("utf-8", errors="ignore") for line in f]

        for n, line in enumerate(events):
            try:
                event = json.loads(line)
            except:

                continue

            actor = event["actor"]
            attrs = event.get("actor_attributes", {})
            if actor is None or attrs.get("type") != "User":
                continue

            key = actor.lower()

            repo = event.get("repository", {})
            info = str(repo.get("owner")), str(repo.get("language")), str(event["type"]), str(repo.get("name")), str(
                repo.get("url"))
            userinfo.append(info)

    return userinfo


def build_db_with_gzip():
    init_db()
    conn = sqlite3.connect('userdata.db')
    c = conn.cursor()

    year = 2014
    month = 3

    for day in range(1,31):
        date_re = re.compile(r"([0-9]{4})-([0-9]{2})-([0-9]{2})-([0-9]+)\.json.gz")

        fn_template = os.path.join("march",
                                   "{year}-{month:02d}-{day:02d}-{n}.json.gz")
        kwargs = {"year": year, "month": month, "day": day, "n": "*"}
        filenames = glob.glob(fn_template.format(**kwargs))

        for filename in filenames:
            c.executemany('INSERT INTO userinfo VALUES (?,?,?,?,?)', handle_gzip_file(filename))

    conn.commit()
    c.close()