# -*- coding: utf-8 -*-
"""
Created on Mon Apr  8 13:51:10 2019

@author: cagurl01
"""

import csv
import pysftp


def append_path(path):
    paths.append(path)
    return None


def dummy(path):
    pass


_connections = []
_join = []
_operations = []
paths = []


with open('./ref/CONNS.csv') as connfile:
    reader = csv.reader(connfile)
    print('Examining connections...')
    for index, row in enumerate(reader):
        if len(row) != 5:
            print('Incorrect number of arguments for row {}! Discarded!'.format(index + 1))
        else:
            row[0] = row[0].lower()
            row[1] = row[1].lower()
            if row[0] not in ('scp', 'sftp'):
                print('Invalid protocol for row {}! Discarded!'.format(index + 1))
            elif not row[4].isdigit():
                print('Invalid port number for row {}! Discarded!'.format(index + 1))
            else:
                row[4] = int(row[4])
                row.append(index + 1)
                _join.append(index + 1)
                _connections.append(row)
with open('./ref/OPS.csv') as opfile:
    reader = csv.reader(connfile)
    print('Examining operations...')
    for index, row in enumerate(reader):
        if len(row) < 4:
            print('Insufficient number of arguments for row {}! Discarded!'.format(index + 1))
        elif not row[0].isdigit():
            print('Invalid connection reference for row {}! Discarded!'.format(index + 1))
        elif row[1].lower() not in ('dir', 'file'):
            print('Invalid target type for row {}! Discarded!'.format(index + 1))
        else:
            row[0] = int(row[0])
            row[1] = row[1].lower()
            row[3] = row[3].lower()
            if row[0] not in _join:
                print('Cannot find referenced connection for row {}! Discarded!'.format(index + 1))
            else:
                _operations.append(row)

if not _operations:
    print('No valid operations found! Operation aborted!')
else:
    with pysftp.Connection(
            host=_connections[0][1],
            username=_connections[0][2],
            password=_connections[0][3],
            port=_connections[0][4]) as conn:
#        print(conn.listdir('/outgoing/for_incoming'))
        conn.walktree('/outgoing/for_incoming', fcallback=append_path, dcallback=dummy, ucallback=dummy)
        for path in paths: 
    if newname:
        newpath = str.join(newdir, newname)
        conn.rename(path, newpath)
    else:
        currdir, currname = path.rsplit('/', 1)
        newpath = str.join(newdir, currname)
        conn.rename(path, newpath)
#        conn.rename('/outgoing/for_incoming/testy.txt', '/outgoing/for_incoming/testier.txt')
#        print(conn.listdir())
#        print(conn.pwd)
#        print(conn.exists('/outgoing/for_incoming/test.txt'))
#        print(conn.isfile('/outgoing/for_incoming/test.txt'))
