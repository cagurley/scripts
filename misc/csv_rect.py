# -*- coding: utf-8 -*-
"""
Created on Wed Sep  5 12:27:53 2018

@author: cagurl01
"""

import csv
import re


with open('temp.csv', newline='') as file:
    reader = csv.reader(file)
    lengths = set()
    whitespace = set()
    for index, row in enumerate(reader):
        if len(row) not in lengths:
            lengths.add(len(row))
        for eindex, entry in enumerate(row):
            space = re.search(r'(?! )\s', entry)
            if space:
                whitespace.add((index + 1, eindex + 1, space[0], entry))
                continue
    print(lengths)
    print(whitespace)
