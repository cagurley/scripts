# -*- coding: utf-8 -*-
"""
Created on Mon Sep 17 14:53:58 2018

@author: cagurl01
"""

import csv
import os


def split_by_length(filepath, new_length=0):
    """
    Splits a CSV file into multiples, each with the specified new_length
    with the exception of the final one, which can be less.
    Provide filepath without extension and new_length as the maximum number
    of rows to be contained in any given file.
    """
    try:
        if not os.access(filepath + '.csv', os.F_OK):
            raise ValueError('CSV file not found.')
    except ValueError as e:
        print(str(e))
    else:
        # Add 97 for 'a'
        file_char_index = 0
        file_length = 0
        file_rows = []
        file_count = 0
        new_count = 0
        new_rows = []
        with open(filepath + '.csv', newline='') as file:
            reader = csv.reader(file)
            for row in reader:
                file_rows.append(row)
        file_length = len(file_rows)
        print('File has ' + str(file_length) + ' rows.')
        for row in file_rows:
            if file_char_index < 26:
                file_count += 1
                new_count += 1
                if new_count < new_length + 1:
                    new_rows.append(row)
                    if file_count < file_length:
                        continue
                with open(filepath + '{}.csv'.format(chr(file_char_index + 97)), 'w', newline='') as newfile:
                    writer = csv.writer(newfile)
                    writer.writerows(new_rows)
                    print('`' + filepath + '{}.csv`'.format(chr(file_char_index + 97)) + ' created.')
                new_rows = []
                new_rows.append(row)
                new_count = 1
                file_char_index += 1
            else:
                print('\nMaximum number of files reached. Please review your file lengths.\n')
                break
        return None
