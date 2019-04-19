# -*- coding: utf-8 -*-
"""
Created on Fri Apr 19 09:54:18 2019

@author: cagurl01
"""

import datetime as dt
import os
from re import search as rs
from time import sleep


while True:
    try:
        core = input("""Enter name for merged file, excluding extension and
'_SYSDATE' if planning to use today's date as a suffix: """)
        append_date = input("""\nDo you want to append an underscore
followed by today's date (Y/n)? """)
        ext = input("\nEnter extension for merged file without separator: ")
        if len(core) == 0 or rs(r'\W', core) or rs(r'\W', ext):
            raise ValueError()

        filename = core
        if not append_date.lower().startswith('n'):
            filename += '_' + dt.datetime.now().strftime('%Y%m%d')
        filename = os.extsep.join([filename, ext])
    except ValueError:
        print('\n***Invalid name or extension. Use only letters, numbers, and underscores. Name must not be empty.***')
        retry = input('Retry (Y/n)? ')
        if retry.lower().startswith('n'):
            break
        print('\n\n')
    else:
        while True:
            try:
                skip_headers = input("\nDo you want to skip header rows (y/N)? ")
                if skip_headers.lower().startswith('y'):
                    how_many = input("How many header rows should be skipped? ")
                    retain_one = input("Do you want to keep header row set for the merged file (Y/n)? ")
                    if not how_many.isdigit():
                        raise ValueError()

                    skip_headers = True
                    how_many = int(how_many)
                    if retain_one.lower().startswith('n'):
                        retain_one = False
                    else:
                        retain_one = True
            except ValueError:
                print('\n***Invalid number of header rows to skip.***')
                retry = input('Retry (Y/n)? ')
                if retry.lower().startswith('n'):
                    break
                print('\n')
            else:
                with os.scandir() as sd:
                    with open(filename, 'wb+') as newfile:
                        for (index, entry) in enumerate(sd):
                            if (entry.name not in (filename, 'merge.py')
                                    and entry.is_file()
                                    and not entry.name.endswith('.exe')):
                                with open(entry.path, 'rb') as file:
                                    if skip_headers and (not retain_one or index > 0):
                                        counter = 0
                                        while counter < how_many:
                                            file.readline()
                                            counter += 1
                                    newfile.writelines(file)
                                if newfile.tell() > 0:
                                    newfile.seek(-1, 2)
                                    if newfile.read(1) != b'\n':
                                        newfile.write(b'\n')
                break
        break
print('\n\nMerge complete.\n')
sleep(1)
