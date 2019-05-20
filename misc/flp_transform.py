# -*- coding: utf-8 -*-
"""
Created on Fri May 17 09:39:45 2019

@author: cagurl01
"""

import csv
import datetime as dt
import os

while True:
    filepath = input("Enter fully qualified file path for WebCAPE CSV file: ")
    filepath = filepath.replace(os.altsep, os.sep)
    if os.path.exists(filepath):
        break
    else:
        print('\nBad path; try again.')
while True:
    filter_date = input("Enter lower bound date as YYYYMMDD or nothing to use sysdate - 1: ")
    if len(filter_date) == 0:
        filter_date = dt.datetime.now().date() - dt.timedelta(1)
        break
    elif len(filter_date) == 8 and filter_date.isdigit():
        filter_date = dt.datetime.strptime(filter_date, '%Y%m%d').date()
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
with open(filepath, newline="") as file:
    reader = csv.DictReader(file, good_keys, 'GARBAGE')
    for row in reader:
        if row['UserAssessmentStatus'].lower() != 'completed':
            continue
        row['EndTime'] = row['EndTime'].split(' +')[0]
        row['EndTime'] = (
            dt.datetime
            .strptime(row['EndTime'], '%m/%d/%Y %I:%M:%S %p')
            .date()
        )
        if row['EndTime'] < filter_date:
            continue
        row['EndTime'] = row['EndTime'].strftime('%m/%d/%Y')
        row['StartTime'] = row['StartTime'].split(' +')[0]
        row['StartTime'] = (
            dt.datetime
            .strptime(row['StartTime'], '%m/%d/%Y %I:%M:%S %p')
            .date()
        )
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
