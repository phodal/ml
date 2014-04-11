#!/usr/bin/env python
# -*- coding: utf-8 -*-

import numpy as np
import matplotlib.mlab as mlab
import matplotlib.pyplot as plt
import parse_data as pd


def simple_draw(label,y):
    plt.figure(figsize=(8, 4))
    plt.plot(y, label=label)
    plt.legend()
    plt.show()


def draw_date(files):
    x = []
    y = []
    mwcs = pd.get_minutes_counts_with_id(files)
    for mwc in mwcs:
        x.append(mwc[0])
        y.append(mwc[1])

    simple_draw(files, mwcs)


if __name__ == '__main__':
    results = pd.get_month_total()
    # results = [6570, 7420, 11274, 12073, 12160, 12378, 12897, 8474, 7984, 12933, 13504, 13763, 13544, 12940, 7119, 7346, 13412, 14008, 12555]
    simple_draw("month",results)
    print results