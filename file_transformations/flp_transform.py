# -*- coding: utf-8 -*-
"""
Created on Fri May 17 09:39:45 2019

@author: cagurl01
"""

import csv
import datetime as dt
import os
import re

while True:
    filepath = input("Enter fully qualified file path for WebCAPE CSV file: ")
    filepath = filepath.replace(os.altsep, os.sep)
    if os.path.exists(filepath):
        break
    else:
        print('\nBad path; try again.')
while True:
    filter_datetime = input("Enter lower bound UTC datetime as 'YYYYMMDDHHMMSS' or nothing to use ./ref/flp_dt.txt: ")
    if len(filter_datetime) == 0:
        with open('./ref/flp_dt.txt') as file:
            filter_datetime = file.readline().strip()
            if not re.match(r'^\d{2}\/\d{2}\/\d{4} \d{2}:\d{2}:\d{2}$', filter_datetime):
                print('Last operation datetime is incorrectly formatted.\nFix to format MM/DD/YYYY HH:MM:SS or enter lower bound datetime at prompt.\n')
                continue
            else:
                filter_datetime = dt.datetime.strptime(filter_datetime, '%m/%d/%Y %H:%M:%S')
        break
    elif len(filter_datetime) == 14 and filter_datetime.isdigit():
        filter_datetime = dt.datetime.strptime(filter_datetime, '%Y%m%d%H%M%S')
        break
    else:
        print('\nBad entry; try again.')

filtered_rows = []
good_keys = [
    'Username',
    'FirstName',
    'LastName',
    'UserId',
    'AssessmentName',
    'UserAssessmentStatus',
    'StartTime',
    'EndTime',
    'DurationInMinutes',
    'Groups',
    'Score',
    'PlacementScaleName'
]
new_datetime = dt.datetime.utcnow().strftime('%m/%d/%Y %H:%M:%S')

with open(filepath, newline="") as file:
    reader = csv.DictReader(file, good_keys, 'GARBAGE')
    for row in reader:
        if row['UserAssessmentStatus'].lower() != 'completed':
            continue
        row['EndTime'] = row['EndTime'].split(' +')[0]
        row['EndTime'] = dt.datetime.strptime(row['EndTime'], '%m/%d/%Y %I:%M:%S %p')
        if row['EndTime'] < filter_datetime:
            continue
        row['EndTime'] = row['EndTime'].strftime('%m/%d/%Y')
        row['StartTime'] = row['StartTime'].split(' +')[0]
        row['StartTime'] = dt.datetime.strptime(row['StartTime'], '%m/%d/%Y %I:%M:%S %p')
        row['StartTime'] = row['StartTime'].strftime('%m/%d/%Y')
        row['DurationInMinutes'] = int(row['DurationInMinutes'])
        row['Score'] = int(row['Score'])
        if row['Score'] < 0:
            row['Score'] = 0
        if row['GARBAGE']:
            del row['GARBAGE']
        filtered_rows.append(row)
with open(filepath, 'w', newline="") as file:
    writer = csv.DictWriter(file, good_keys)
    writer.writerows(filtered_rows)
with open('./ref/flp_dt.txt', 'w') as file:
    file.write(new_datetime + '\n')
