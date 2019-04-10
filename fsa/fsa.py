# -*- coding: utf-8 -*-
"""
Created on Mon Apr  8 13:51:10 2019

@author: cagurl01
"""

import csv
import datetime as dt
import pysftp


class ConnDirectives:
    def __init__(self, ref, protocol, host, username, password, port):
        self.ref = ref
        self.protocol = protocol
        self.host = host
        self.username = username
        self.password = password
        self.port = port
        self.ops = []
        self.access_time = None
        self.previous_time = None
        self.set_prev_time

    def add_op(self, op):
        if op.conn_ref == self.ref:
            self.ops.append(op)
        return None

    def cycle_times(self):
        if self.access_time:
            rows = []
            with open('ref/TIMES.csv') as file:
                reader = csv.reader(file)
                for row in reader:
                    rows.append(row)
            for index, row in enumerate(rows):
                if len(row) != 2 or row[0] == self.ref:
                    rows.pop(index)
            rows.append([self.ref, self.access_time])
            self.previous_time = self.access_time
            self.access_time = None
            with open('ref/TIMES.csv', 'w') as file:
                writer = csv.writer(file)
                writer.writerows(rows)
        return None

    def set_access_time(self):
        self.access_time = dt.datetime.today()
        return None

    def set_prev_time(self):
        with open('ref/TIMES.csv') as file:
            reader = csv.reader(file)
            for row in reader:
                if len(row) == 2 and row[0] == self.ref:
                    try:
                        self.previous_time = dt.datetime.strptime(row[1], '%c')
                        return None
                    except ValueError:
                        self.previous_time = dt.datetime.today()
            self.previous_time = dt.datetime.today()
            return None

class OpDirectives:
    def __init__(self, conn_ref, target_type, target_path, operation, *args):
        self.conn_ref = conn_ref
        self.target_type = target_type
        self.target_path = target_path
        self.operation = operation
        self.args = [*args]

    def validate(self):
        if (self.target_type == 'dir'
                and self.operation == 'rename_files'
                and len(self.args) == 1):
            return True
        else:
            return False


def dummy(path):
    pass


### Command functions
# Command selection
def choose_func(conn, conndir, opdir):
    funcname = (conndir.protocol + '_'
                + opdir.target_type + '_'
                + opdir.operation)
    if funcname == 'sftp_dir_rename_files':
        sftp_dir_rename_files(conn, opdir)
    return None


# SFTP commands
def sftp_dir_rename_files(pysftp_conn, opdir):
    paths = []
    pysftp_conn.walktree('/outgoing/for_incoming',
                         fcallback=paths.append,
                         dcallback=dummy,
                         ucallback=dummy,
                         recurse=False)
    for path in paths:
        currdir, currname = path.rsplit('/', 1)
        newpath = '/'.join([opdir.args[0], currname])
        conn.rename(path, newpath)
    return None


_conndir = []
_join = []
_opdir = []


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
                _join.append(index + 1)
                _conndir.append(ConnDirectives(index + 1, *row))
with open('./ref/OPS.csv') as opfile:
    reader = csv.reader(opfile)
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
                opdir = OpDirectives(*row)
                if not opdir.validate():
                    print('Specified operation is invalid for row {}! Discarded!'.format(index + 1))
                else:
                    _opdir.append(opdir)

if not _opdir:
    print('No valid operations found! Operation aborted!')
else:
    for conndir in _conndir:
        for index, opdir in enumerate(_opdir):
            if conndir.ref == opdir.conn_ref:
                conndir.add_op(opdir)
                _opdir.pop(index)
    for index, conndir in enumerate(_conndir):
        if not conndir.ops:
            _conndir.pop(index)

    for conndir in _conndir:
        try:
            if conndir.protocol == 'sftp':
                with pysftp.Connection(
                        host=conndir.host,
                        username=conndir.username,
                        password=conndir.password,
                        port=conndir.port) as conn:
                    conndir.set_access_time()
                    for opdir in conndir.ops:
                        choose_func(conn, conndir, opdir)
        except pysftp.SSHException:
            print('SSH exception for host {} raised! Check for incorrect connection directives or missing key in `~./.ssh/known_hosts`.'.format(conndir.host))
#print('hold')
