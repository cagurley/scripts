# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import csv
import datetime
import os
import re


all_rows = []
with os.scandir() as directory:
    for item in directory:
        if re.match(r'\w+[.]csv$', item.name):
            with open(item.name, newline='') as file:
                file_rows = []
                reader = csv.reader(file)
                for row in reader:
                    file_rows.append(row)
                file_rows.pop(0)
                file_rows.pop(0)
                for row in file_rows:
                    all_rows.append(row)
            with open(item.name, 'w', newline='') as file:
                writer = csv.writer(file)
                writer.writerows(file_rows)
with open('applcat_' + datetime.date.today().strftime('%m%d%y') + '.csv', 'w', newline='') as combined_file:
    writer = csv.writer(combined_file)
    writer.writerows(all_rows)
