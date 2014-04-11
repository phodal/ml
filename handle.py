#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import dateutil.parser
import numpy as np
import matplotlib.mlab as mlab
import matplotlib.pyplot as plt


def parse_data(jsonfile):
    f = open(jsonfile, "r")
    dataarray = []
    datacount = 0

    for line in open(jsonfile):
        line = f.readline()
        lin = json.loads(line)
        date = dateutil.parser.parse(lin["created_at"])
        datacount += 1
        dataarray.append(date.minute)

    minuteswithcount = [(x, dataarray.count(x)) for x in set(dataarray)]
    f.close()
    print datacount
    return minuteswithcount


def draw_date(files):
    x = []
    y = []
    mwcs = parse_data(files)
    for mwc in mwcs:
        x.append(mwc[0])
        y.append(mwc[1])

    plt.figure(figsize=(8, 4))
    plt.plot(x, y, label=files)
    plt.legend()
    plt.show()

# draw_date("data/2014-01-01-0.json")
parse_data("data/2014-02-01-0.json")
parse_data("data/2014-02-02-0.json")
parse_data("data/2014-02-03-0.json")
parse_data("data/2014-02-04-0.json")
parse_data("data/2014-02-05-0.json")
parse_data("data/2014-02-06-0.json")
parse_data("data/2014-02-07-0.json")
parse_data("data/2014-02-08-0.json")
parse_data("data/2014-02-09-0.json")
parse_data("data/2014-02-10-0.json")
parse_data("data/2014-02-11-0.json")
parse_data("data/2014-02-12-0.json")
parse_data("data/2014-02-13-0.json")
parse_data("data/2014-02-14-0.json")
parse_data("data/2014-02-15-0.json")
parse_data("data/2014-02-16-0.json")
parse_data("data/2014-02-17-0.json")
parse_data("data/2014-02-18-0.json")
parse_data("data/2014-02-19-0.json")
parse_data("data/2014-02-20-0.json")