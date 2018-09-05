# -*- coding: utf-8 -*-
"""
Created on Tue Jul 31 16:54:26 2018

@author: cagurl01
"""

import csv
import datetime


# File content vars
header = None
special_visits = []
cvp = []
academic_appt = []
accolade = []
porter_breakfast = []
pres_outreach = []
shadow_visits = []
oyes = []
ucp = []
tours_only = []
outlook_day = []
cpd = []
porter_reception = []
asd = []
fin_aid_prep = []
o_and_s = []
tcn = []
lmr = []
unexpected = []
# Dicts to split into separate files
meeting_num_split = {
    range(1, 13): cvp,
    range(13, 25): academic_appt
}
event_num_split = {
    '000147199': special_visits,
    '000147419': meeting_num_split,  # Refers to other dict for further split
    '000147424': accolade,
    '000147497': porter_breakfast,
    '000147518': pres_outreach,
    '000147519': shadow_visits,
    '000147520': oyes,
    '000147521': ucp,
    '000147522': tours_only,
    '000147830': outlook_day,
    '000147862': cpd,
    '000147886': porter_reception,
    '000149162': asd,
    '000152799': fin_aid_prep,
    '000152800': o_and_s,
    '000152835': tcn,
    '000153001': lmr
}
file_name_bases = {
    'Special_Visits': special_visits,
    'CVP': cvp,
    'Academic_Appt': academic_appt,
    'Accolade': accolade,
    'Porter_Breakfast': porter_breakfast,
    'Pres_Outreach': pres_outreach,
    'Shadow_Visits': shadow_visits,
    'OYES': oyes,
    'UCP': ucp,
    'Tours_Only': tours_only,
    'Outlook Day': outlook_day,
    'CPD': cpd,
    'Porter_Reception': porter_reception,
    'ASD': asd,
    'Fin_Aid_Prep': fin_aid_prep,
    'O&S': o_and_s,
    'TCN': tcn,
    'LMR': lmr,
    'unexpected': unexpected
}

# Reading and splitting single file
with open('annual_event_data_20180731.csv', newline='') as file:
    reader = csv.reader(file)
    for index, row in enumerate(reader):
        if index == 0:
            header = row
        elif row[0] in event_num_split.keys():
            if isinstance(event_num_split[row[0]], dict):
                for key in meeting_num_split.keys():
                    if int(row[1]) in key:
                        meeting_num_split[key].append(row)
                        break
            else:
                event_num_split[row[0]].append(row)
        else:
            unexpected.append(row)
for name, body_rows in file_name_bases.items():
    if len(body_rows) > 0:
        filename = name + '_' + (datetime.date.today() + datetime.timedelta(days=-1)).strftime('%Y%m%d')
        with open(filename + '.csv', 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(header)
            writer.writerows(body_rows)
