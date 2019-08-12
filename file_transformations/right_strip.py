# -*- coding: utf-8 -*-
"""
Created on Wed Aug  7 13:00:59 2019

@author: cagurl01
"""

import os
import re

from datetime import datetime as dt


dirpath = None
filepath = None
charnum = '2'
lines = []
while True:
    dirpath = input('\nPlease enter source directory path for file to strip:\n')
    dirpath = dirpath.replace(os.altsep, os.sep)
    if os.path.exists(dirpath):
        while True:
            filename = input('\nPlease enter name for nonbinary source file, including extension:\n')
            filepath = os.path.join(dirpath, filename)
            if os.path.exists(filepath):
                while True:
                    usernum = input('\nPlease enter number of characters to strip per line (default: 2):\n')
                    if len(usernum) > 0 and usernum.isdigit():
                        charnum = usernum
                        break
                    elif len(usernum) == 0:
                        break
                    else:
                        print('\nAn integer value was not entered; try again.\n\n')
                break
            else:
                print('\nFile does not exist in the given directory; try again.\n\n')
        break
    else:
        print('\nPath does not exist; try again.\n\n')
pattern = r'.{' + charnum + r'}(\r?\n?)$'
with open(filepath) as file:
    for line in file.readlines():
        lines.append(re.sub(pattern, r'\1', line))
newpath = os.path.join(dirpath, 'output_{}.txt'.format(dt.today().strftime('%Y%m%d%H%M%S')))
with open(newpath, 'w') as file:
    file.writelines(lines)
print(f'\nNew file generated at `{newpath}`.\n')