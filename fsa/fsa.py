# -*- coding: utf-8 -*-
"""
Created on Mon Apr  8 13:51:10 2019

@author: cagurl01
"""

import csv
import datetime as dt
import os
import pysftp
import re
from time import sleep


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
        self.set_prev_time()

    def add_op(self, op):
        if op.conn_ref == self.ref:
            self.ops.append(op)
        return None

    def cycle_times(self):
        if self.access_time:
            rows = []
            with open('ref/TIMES.csv', newline='') as file:
                reader = csv.reader(file)
                for row in reader:
                    rows.append(row)
            for index, row in enumerate(rows):
                if len(row) != 2 or row[0] == str(self.ref):
                    rows.pop(index)
            rows.append([str(self.ref), self.access_time.strftime('%c')])
            self.previous_time = self.access_time
            self.access_time = None
            with open('ref/TIMES.csv', 'w', newline='') as file:
                writer = csv.writer(file)
                writer.writerows(rows)
        return None

    def set_access_time(self):
        self.access_time = dt.datetime.today()
        return None

    def set_prev_time(self):
        with open('ref/TIMES.csv', newline='') as file:
            reader = csv.reader(file)
            for row in reader:
                if len(row) == 2 and row[0] == str(self.ref):
                    try:
                        self.previous_time = dt.datetime.strptime(row[1], '%c')
                        return None
                    except ValueError:
                        self.previous_time = dt.datetime.today()
            self.previous_time = dt.datetime.today()
            return None


class OpDirectives:
    def __init__(self, conn_ref, target_type, target_path, operation, pattern, *args):
        self.conn_ref = conn_ref
        self.target_type = target_type
        self.target_path = target_path
        self.operation = operation
        if pattern:
            self.pattern = re.compile(pattern)
        else:
            self.pattern = None
        self.args = [*args]

    def validate(self):
        if (self.target_type == 'dir'
            and ((self.operation == 'rename_files'
                  and len(self.args) == 1)
                 or (self.operation in ('copy_to', 'move_to', 'copy_from', 'move_from')
                     and len(self.args) == 2))):
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
    elif funcname == 'sftp_dir_copy_to':
        sftp_dir_copy_to(conn, conndir, opdir)
    elif funcname == 'sftp_dir_move_to':
        sftp_dir_move_to(conn, conndir, opdir)
    elif funcname == 'sftp_dir_copy_from':
        sftp_dir_copy_from(conn, conndir, opdir)
    elif funcname == 'sftp_dir_move_from':
        sftp_dir_move_from(conn, conndir, opdir)
    return None


# SFTP commands
def sftp_dir_rename_files(pysftp_conn, opdir):
    paths = []
    pysftp_conn.walktree(opdir.target_path,
                         fcallback=paths.append,
                         dcallback=dummy,
                         ucallback=dummy,
                         recurse=False)
    for path in paths:
        currdir, currname = path.rsplit('/', 1)
        if opdir.pattern and not re.match(opdir.pattern, currname):
            continue
        newpath = '/'.join([opdir.args[0], currname])
        conn.rename(path, newpath)
    return None


def sftp_dir_copy_to(pysftp_conn, conndir, opdir):
    files = []
    with os.scandir(opdir.args[0]) as local:
        for file in local:
            if opdir.pattern and not re.match(opdir.pattern, file.name):
                continue
            if opdir.args[1].lower() == 'yes':
                fdt = dt.datetime.fromtimestamp(file.stat().st_ctime)
                if fdt > conndir.previous_time and fdt <= conndir.access_time:
                    files.append(file)
            else:
                files.append(file)
    for file in files:
        newpath = '/'.join([opdir.target_path, file.name])
        pysftp_conn.put(file.path, newpath, preserve_mtime=True)
    return files


def sftp_dir_move_to(pysftp_conn, conndir, opdir):
    files = sftp_dir_copy_to(pysftp_conn, conndir, opdir)
    for file in files:
        os.remove(file.path)
    return None


def sftp_dir_copy_from(pysftp_conn, conndir, opdir):
    paths = []
    matched_paths = []
    pysftp_conn.walktree(opdir.target_path,
                         fcallback=paths.append,
                         dcallback=dummy,
                         ucallback=dummy,
                         recurse=False)
    if opdir.pattern:
        for path in paths:
            currdir, currname = path.rsplit('/', 1)
            if re.match(opdir.pattern, currname):
                matched_paths.append(path)
    else:
        matched_paths = paths
    if opdir.args[1].lower() == 'yes':
        paths = matched_paths
        matched_paths = []
        for path in paths:
            fdt = dt.datetime.fromtimestamp(pysftp_conn.stat(path).st_mtime)
            if fdt > conndir.previous_time and fdt <= conndir.access_time:
                matched_paths.append(path)
    for path in matched_paths:
        currdir, currname = path.rsplit('/', 1)
        newpath = os.sep.join([opdir.args[0], currname])
        conn.get(path, newpath, preserve_mtime=True)
    return matched_paths


def sftp_dir_move_from(pysftp_conn, conndir, opdir):
    paths = sftp_dir_copy_from(pysftp_conn, conndir, opdir)
    for path in paths:
        pysftp_conn.remove(path)
    return None


# Main try clause for infinite loop
# Should be terminated with keyboard interrupt
try:
    while True:
        _conndir = []
        _join = []
        _opdir = []
        start = dt.datetime.now()
        next_start = start + dt.timedelta(minutes=30)

        print('Scanning initiated! Timestamp: {}'.format(str(start)))
        with open('./ref/CONNS.csv', newline='') as connfile:
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
        with open('./ref/OPS.csv', newline='') as opfile:
            reader = csv.reader(opfile)
            print('Examining operations...')
            for index, row in enumerate(reader):
                if len(row) < 5:
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
                for opdir in _opdir:
                    if conndir.ref == opdir.conn_ref:
                        conndir.add_op(opdir)
            _opdir = []
            conns_with_ops = []
            for conndir in _conndir:
                if conndir.ops:
                    conns_with_ops.append(conndir)
            _conndir = conns_with_ops

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
                    conndir.cycle_times()
                except pysftp.SSHException:
                    # Note that this doesn't handle subsequent AttirubteError
                    # thrown by context manager. Needs future fix.
                    print('SSH exception for host {} raised! Check for incorrect connection directives or missing key in `~./.ssh/known_hosts`.'.format(conndir.host))
                    continue

        sleep_interval = (next_start - dt.datetime.now()).seconds
        if sleep_interval > 0:
            print('Operations completed! Next scan will initiate at {}.'.format(str(next_start)))
            sleep(sleep_interval)
        else:
            print('Operations have taken longer than 30 minutes to complete! Review directives or source.')
except KeyboardInterrupt:
    print('Agent activity interrupted! Resume when ready.')
