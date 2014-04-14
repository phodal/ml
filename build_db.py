import json
import sqlite3
import os
import glob
import gzip
import logging
from datetime import date, timedelta
import re

import redis
r = redis.StrictRedis(host='localhost', port=6379, db=1)

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

def _format(key):
    return "{0}:{1}".format("od", key)

def build_db_with_redis():
    year = 2014
    month = 3
    pipe = r.pipeline()

    for day in range(1, 31):
        date_re = re.compile(r"([0-9]{4})-([0-9]{2})-([0-9]{2})-([0-9]+)\.json.gz")

        fn_template = os.path.join("march",
                                   "{year}-{month:02d}-{day:02d}-{n}.json.gz")
        kwargs = {"year": year, "month": month, "day": day, "n": "*"}
        filenames = glob.glob(fn_template.format(**kwargs))

        for filename in filenames:
            userinfo = []
            year, month, day, hour = map(int, date_re.findall(filename)[0])
            weekday = date(year=year, month=month, day=day).strftime("%w")

            with gzip.GzipFile(filename) as f:
                events = [line.decode("utf-8", errors="ignore") for line in f]
                count = len(events)

                for n, line in enumerate(events):

                    event = json.loads(line)

                    actor = event["actor"]
                    attrs = event.get("actor_attributes", {})
                    if actor is None or attrs.get("type") != "User":
                        # This was probably an anonymous event (like a gist event)
                        # or an organization event.
                        continue

                    key = actor.lower()
                    evttype = event["type"]
                    nevents = 1
                    contribution = evttype in ["IssuesEvent", "PullRequestEvent","PushEvent"]

                    pipe.incr(_format("total"), nevents)
                    pipe.hincrby(_format("day"), weekday, nevents)
                    pipe.hincrby(_format("hour"), hour, nevents)
                    pipe.zincrby(_format("user"), key, nevents)
                    pipe.zincrby(_format("event"), evttype, nevents)

                    # Event histograms.
                    pipe.hincrby(_format("event:{0}:day".format(evttype)), weekday,
                                 nevents)
                    pipe.hincrby(_format("event:{0}:hour".format(evttype)), hour,
                                 nevents)

                    # User schedule histograms.
                    pipe.hincrby(_format("user:{0}:day".format(key)), weekday, nevents)
                    pipe.hincrby(_format("user:{0}:hour".format(key)), hour, nevents)

                    # User event type histogram.
                    pipe.zincrby(_format("user:{0}:event".format(key)), evttype,
                                 nevents)
                    pipe.hincrby(_format("user:{0}:event:{1}:day".format(key,
                                                                         evttype)),
                                 weekday, nevents)
                    pipe.hincrby(_format("user:{0}:event:{1}:hour".format(key,
                                                                          evttype)),
                                 hour, nevents)

                    # Parse the name and owner of the affected repository.
                    repo = event.get("repository", {})
                    owner, name, org = (repo.get("owner"), repo.get("name"),
                                        repo.get("organization"))
                    if owner and name:
                        repo_name = "{0}/{1}".format(owner, name)
                        pipe.zincrby(_format("repo"), repo_name, nevents)

                        # Save the social graph.
                        pipe.zincrby(_format("social:user:{0}".format(key)),
                                     repo_name, nevents)
                        pipe.zincrby(_format("social:repo:{0}".format(repo_name)),
                                     key, nevents)

                        # Do we know what the language of the repository is?
                        language = repo.get("language")
                        if language:
                            # Which are the most popular languages?
                            pipe.zincrby(_format("lang"), language, nevents)

                            # Total number of pushes.
                            if evttype == "PushEvent":
                                pipe.zincrby(_format("pushes:lang"), language, nevents)

                            pipe.zincrby(_format("user:{0}:lang".format(key)),
                                         language, nevents)

                            # Who are the most important users of a language?
                            if contribution:
                                pipe.zincrby(_format("lang:{0}:user".format(language)),
                                             key, nevents)

                pipe.execute()