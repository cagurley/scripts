# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import csv
import datetime as dt
import re
import tempfile as tf


class FileLayout:
    """
    Parameters: layout_name, filetype, email_index, email_end_index=0
    layout_name should be str, will be converted to lower
    file_type should be 'flat' or 'csv', will be converted to lower
    email_index is index of column or beginning byte index of email field
    email_end_index should equal byte index just after email field
        in flat files; will be used in a slice, so length of field should
        equal email_index - email_end_index
    """
    def __init__(self, layout_name, filetype, email_index, email_end_index=0, name_indices=None, first_last=None, date_indices=None, format_mask=None):
        self.layout_name = layout_name
        self.filetype = filetype
        self.email_index = email_index
        self.email_end_index = email_end_index
        self.name_indices = name_indices
        self.first_last = first_last
        self.date_indices = date_indices
        self.format_mask = format_mask

    def __str__(self):
        attributes = '\n  Layout Name: {}\n  File Type: {}\n  Email Index: {}'.format(self.layout_name, self.filetype, self.email_index)
        if self.email_end_index != 0:
            attributes += '\n  Email End Index: {}'.format(self.email_end_index)
        if self.name_indices is not None:
            attributes += '\n  Name Indices: {}'.format(self.name_indices)
        if self.first_last is not None:
            attributes += '\n  First/Last Indices: {}'.format(self.first_last)
        if self.date_indices is not None:
            attributes += '\n  Date Indices: {}'.format(self.date_indices)
        if self.format_mask is not None:
            attributes += '\n  Format Mask: {}'.format(self.format_mask)
        return attributes


# Below file should contain explicit email domains as unquoted strings on separate lines
EXPLICIT_DOMAINS = set()
with open('ref/EXPLICIT_DOMAINS.txt') as file:
    for line in file.readlines():
        EXPLICIT_DOMAINS.add(line.strip())
# Below file should contain valid district names as unquoted strings on separate lines
VALID_DISTRICTS = set()
with open('ref/VALID_DISTRICTS.txt') as file:
    for line in file.readlines():
        VALID_DISTRICTS.add(line.strip())
# Below file should contain bogus name pairs as individual rows of length two consisting of first name then last name
BOGUS_NAMES = set()
with open('ref/BOGUS_NAMES.csv') as file:
    reader = csv.reader(file)
    for row in reader:
        BOGUS_NAMES.add((row[0], row[1]))
# See class constructor above for details on parameters
FILE_LAYOUTS = [
    FileLayout('act', 'flat', 550, 600, ((2, 27), (27, 43), (43, 44)), ((27, 43), (2, 27))),
    FileLayout('pcl', 'flat', 199, 249, ((2, 22), (22, 23), (37, 62)), ((2, 22), (37, 62)), ((86, 94),), '%Y%m%d'),
    FileLayout('sat', 'flat', 398, 526, ((6, 41), (41, 76), (76, 77)), ((41, 76), (6, 41))),
    FileLayout('cpd', 'csv', 3, 0, (0, 1), (0, 1), (4,), '%m/%d/%Y'),
    FileLayout('cfa', 'csv', 23, 0, (3, 4, 5), (3, 5), (15,), '%m/%d/%Y'),
    FileLayout('npc', 'csv', 11, 0, (3, 4, 5), (3, 5)),
    FileLayout('gsp', 'csv', 14, 0, (3, 4, 5), (3, 5)),
    FileLayout('gsa', 'csv', 4, 0, (0, 1, 2, 3), (1, 0)),
    FileLayout('vis', 'csv', 8, 0, (6, 7), (6, 7))
]


def strip_csv(filetype, filepath):
    try:
        if filetype.lower() != 'csv':
            raise ValueError("File type must be 'csv'.")
    except ValueError as e:
        print(str(e))
    else:
        with tf.TemporaryFile('w+', newline='') as tempfile:
            treader = csv.reader(tempfile)
            twriter = csv.writer(tempfile)
            with open(filepath, newline='') as file:
                reader = csv.reader(file)
                for row in reader:
                    for index, entry in enumerate(row):
                        row[index] = entry.strip()
                    twriter.writerow(row)
            with open(filepath, 'w', newline='') as file:
                tempfile.seek(0)
                writer = csv.writer(file)
                writer.writerows(treader)


def email_audit(filetype, filepath, email_index, email_end_index=0):
    try:
        if filetype.lower() not in ('csv', 'flat'):
            raise ValueError("Specify 'csv' or 'flat' as file type.")
        if (not isinstance(email_index, int)
                or not isinstance(email_end_index, int)):
            raise TypeError('Email indexes must be integers.')
    except ValueError as e:
        print(str(e))
    except TypeError as e:
        print(str(e))
    else:
        if filetype.lower() == 'csv':
            newline = ''
        elif filetype.lower() == 'flat':
            newline = None
        with open(filepath, newline=newline) as infile:
            lines = None
            if filetype.lower() == 'csv':
                lines = csv.reader(infile)
            elif filetype.lower() == 'flat':
                lines = infile.readlines()
            audit_rows = []
            for index, row in enumerate(lines):
                email = None
                if filetype.lower() == 'csv':
                    email = row[email_index]
                elif filetype.lower() == 'flat':
                    email = row[email_index:email_end_index].rstrip()
                if email == '':
                    continue
                elif re.search(r'\s', email):
                    audit_rows.append((index + 1, email, 'EMAIL', 'Whitespace Error'))
                    continue
                elif not re.match(r"[\d\w!#$%&'*+-/=?^`{|}~]+(\.[\d\w!#$%&'*+-/=?^`{|}~]+)*@(?!-)[-\da-zA-Z]+(?<!-)(\.(?!-)[-\da-zA-Z]+(?<!-))+$", email, re.A):
                    audit_rows.append((index + 1, email, 'EMAIL', 'Malformation Error'))
                    continue
                domain = re.search(r'@(.+)$', email.lower())
                if domain:
                    if domain[1] in EXPLICIT_DOMAINS:
                        continue
                    else:
                        county_check = re.match(r'stu\.(.+?)\.kyschools\.us$', domain[1])
                        if county_check and county_check[1] in VALID_DISTRICTS:
                            continue
                audit_rows.append((index + 1, email, 'EMAIL', 'Domain Error'))
            with open('to_check.csv', 'w', newline='') as audit:
                writer = csv.writer(audit)
                writer.writerow(['RN', 'CONTENT', 'TYPE', 'ERROR'])
                for row in audit_rows:
                    writer.writerow(row)
        return None


def name_audit(filetype, filepath, name_indices, first_last=None):
    """
    Checks for name issues
    DO NOT call except immediately after email_audit() until further notice
    """
    try:
        if filetype.lower() not in ('csv', 'flat'):
            raise ValueError("Specify 'csv' or 'flat' as file type.")
        elif filetype.lower() == 'csv':
            for index in name_indices:
                if not isinstance(index, int):
                    raise TypeError('Indices must be single integers.')
        elif filetype.lower() == 'flat':
            for index_pair in name_indices:
                if not isinstance(index_pair, tuple) or len(index_pair) != 2:
                    raise TypeError('Indices must be two-integer tuples.')
                else:
                    for index in index_pair:
                        if not isinstance(index, int):
                            raise TypeError('Indices must be two-integer tuples.')
        if (first_last is not None
                and (not isinstance(first_last, tuple) or len(first_last) != 2)):
            raise TypeError('First and last must be given as a two-tuple.')
        else:
            if filetype.lower() == 'csv':
                for index in first_last:
                    if not isinstance(index, int):
                        raise TypeError('First and last must be integers for CSV type.')
            elif filetype.lower() == 'flat':
                for index_pair in first_last:
                    if not isinstance(index_pair, tuple):
                        raise TypeError('First and last must be integer two-tuples for FLAT type.')
                    else:
                        for index in index_pair:
                            if not isinstance(index, int):
                                raise TypeError('First and last must be integer two-tuples for FLAT type.')
    except ValueError as e:
        print(str(e))
    except TypeError as e:
        print(str(e))
    else:
        if filetype.lower() == 'csv':
            newline = ''
        elif filetype.lower() == 'flat':
            newline = None
        with open(filepath, newline=newline) as infile:
            lines = None
            if filetype.lower() == 'csv':
                lines = csv.reader(infile)
            elif filetype.lower() == 'flat':
                lines = infile.readlines()
            audit_rows = []
            for index, row in enumerate(lines):
                names = []
                fl_names = []
                if filetype.lower() == 'csv':
                    for name_index in name_indices:
                        names.append(row[name_index])
                    for name_index in first_last:
                        fl_names.append(row[name_index].strip().lower())
                elif filetype.lower() == 'flat':
                    for index_pair in name_indices:
                        names.append(row[index_pair[0]:index_pair[1]].rstrip())
                    for index_pair in first_last:
                        fl_names.append(row[index_pair[0]:index_pair[1]].strip().lower())
                for name in names:
                    if name != '' and re.search(r"[^-'\w]", name):
                        audit_rows.append((index + 1, name, 'NAME', 'Character Error'))
                        break
                if len(fl_names[0]) < 2 or len(fl_names[1]) < 2:
                    audit_rows.append((index + 1, fl_names[0] + ' ' + fl_names[1], 'NAME', 'Length Error'))
                elif tuple(fl_names) in BOGUS_NAMES:
                    audit_rows.append((index + 1, fl_names[0] + ' ' + fl_names[1], 'NAME', 'Bogus Name Error'))
            with open('to_check.csv', 'a', newline='') as audit:
                writer = csv.writer(audit)
                for row in audit_rows:
                    writer.writerow(row)
        return None


def date_audit(filetype, filepath, date_indices, format_mask):
    try:
        if filetype.lower() not in ('csv', 'flat'):
            raise ValueError("Specify 'csv' or 'flat' as file type.")
        elif filetype.lower() == 'csv':
            for index in date_indices:
                if not isinstance(index, int):
                    raise TypeError('Indices must be single integers.')
        elif filetype.lower() == 'flat':
            for index_pair in date_indices:
                if not isinstance(index_pair, tuple) or len(index_pair) != 2:
                    raise TypeError('Indices must be two-integer tuples.')
                else:
                    for index in index_pair:
                        if not isinstance(index, int):
                            raise TypeError('Indices must be two-integer tuples.')
    except ValueError as e:
        print(str(e))
    except TypeError as e:
        print(str(e))
    else:
        if filetype.lower() == 'csv':
            newline = ''
        elif filetype.lower() == 'flat':
            newline = None
        with open(filepath, newline=newline) as infile:
            lines = None
            if filetype.lower() == 'csv':
                lines = csv.reader(infile)
            elif filetype.lower() == 'flat':
                lines = infile.readlines()
            audit_rows = []
            for index, row in enumerate(lines):
                date_strings = []
                if filetype.lower() == 'csv':
                    for date_index in date_indices:
                        date_strings.append(row[date_index])
                elif filetype.lower() == 'flat':
                    for index_pair in date_indices:
                        date_strings.append(row[index_pair[0]:index_pair[1]])
                for date_string in date_strings:
                    date = None
                    try:
                        date = dt.datetime.strptime(date_string, format_mask)
                    except ValueError:
                        audit_rows.append((index + 1, date_string, 'DATE', 'Invalid Date Error'))
                    else:
                        if dt.datetime.today().year - date.year < 0:
                            audit_rows.append((index + 1, date_string, 'DATE', 'Future Date Error'))
                        elif dt.datetime.today().year - date.year > 90:
                            audit_rows.append((index + 1, date_string, 'DATE', 'Old Date Error'))
            with open('to_check.csv', 'a', newline='') as audit:
                writer = csv.writer(audit)
                for row in audit_rows:
                    writer.writerow(row)
        return None


def current_layouts():
    list_view = []
    for file_layout in FILE_LAYOUTS:
        summary = "  {}: {}".format(
            file_layout.layout_name,
            file_layout.filetype
        )
        list_view.append(summary)
    print('\nCurrently defined layouts:\n')
    list_view.sort()
    for summary in list_view:
        print(summary)
    print('\nTo see full details for a layout, use layout_def(layout_name).\n')
    return None


def layout_def(layout_name):
    for file_layout in FILE_LAYOUTS:
        if layout_name.lower() == file_layout.layout_name:
            print(file_layout)
            print('')
            return None
        else:
            continue
    print('\nFile layout not defined.\n')
    return None


def layout_audit(layout_name, filepath):
    """
    Parameters: layout_name, filepath
    Checks email for predefined layouts
    """
    print('\nChecking for layout definition...')
    for file_layout in FILE_LAYOUTS:
        if layout_name.lower() == file_layout.layout_name:
            print('Definition found. Parsing file...')
            if file_layout.filetype == 'csv':
                strip_csv(
                    file_layout.filetype,
                    filepath
                )
            email_audit(
                file_layout.filetype,
                filepath,
                file_layout.email_index,
                email_end_index=file_layout.email_end_index
            )
            name_audit(
                file_layout.filetype,
                filepath,
                file_layout.name_indices,
                file_layout.first_last
            )
            if (file_layout.date_indices is not None
                    and file_layout.format_mask is not None):
                date_audit(
                    file_layout.filetype,
                    filepath,
                    file_layout.date_indices,
                    file_layout.format_mask
                )
            print('Audit complete.\n')
            return None
        else:
            continue
    print('File layout not defined.\n')
    return None
