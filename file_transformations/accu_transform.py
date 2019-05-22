# -*- coding: utf-8 -*-
"""
Created on Tue May 21 10:57:57 2019

@author: cagurl01
"""

import csv
import os


while True:
    filepath = input("Enter fully qualified file path for Accuplacer comma-separated TXT file: ")
    filepath = filepath.replace(os.altsep, os.sep)
    if os.path.exists(filepath):
        break
    else:
        print('\nBad path; try again.')

try:
    rows = []
    with open(filepath, newline='') as file:
        reader = csv.reader(file)
        for row in reader:
            if len(row) < 69:
                raise ValueError("Input rows are too short, needing to be 69 values or greater in length.")
            test_names = {
                27: row[27].partition(' '),
                31: row[31].partition(' '),
                35: row[35].partition(' '),
                39: row[39].partition(' '),
                43: row[43].partition(' '),
                47: row[47].partition(' '),
                51: row[51].partition(' '),
                55: row[55].partition(' '),
                59: row[59].partition(' '),
                63: row[63].partition(' '),
                67: row[67].partition(' ')                
            }
            for (index, tup) in test_names.items():
                if len(tup[2]) > 0:
                    row[index] = tup[2]
                else:
                    row[index] = tup[0]
            rows.append(row)
except ValueError as e:
    print(str(e))
else:
    with open(filepath, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(rows)
