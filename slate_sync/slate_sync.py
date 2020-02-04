# -*- coding: utf-8 -*-
"""
Created on Fri Nov  8 10:58:37 2019

@author: cagurl01
"""

from tempfile import TemporaryFile
import csv
import cx_Oracle as cxo
import datetime as dt
import json
import os
import pyodbc
import shutil
import sqlite3


def validate_keys(srcdict, keys=tuple()):
    for key in keys:
        if key not in srcdict:
            return False
    for key in srcdict:
        if key not in keys:
            return False
    return True


def prep_sql_vals(*args):
    prepped = []
    for value in args:
        if isinstance(value, str):
            prepped.append('\'' + value + '\'')
        else:
            prepped.append(str(value))
    return prepped


def query_to_csv(filename, cursor, return_indices=None, archivename=None):
    """
    If archivename is supplied, it should be a path string for
    an "archived" copy of the file.
    """
    return_data = []
    with TemporaryFile('r+', newline='') as tfile:
        twriter = csv.writer(tfile)
        header = []
        should_return = False
        for row in cursor.description:
            if len(row) > 0:
                header.append(row[0])
        twriter.writerow(header)
        if return_indices:
            for index in return_indices:
                if len(header) >= index:
                    should_return = True

        counter = 0
        while True:
            frows = cursor.fetchmany(500)
            if not frows:
                print(f'\nFetched and wrote {cursor.rowcount} total rows.\n\n')
                break
            print(f'Fetched and wrote from row {counter*500 + 1}...')
            counter += 1
            twriter.writerows(frows)
            if should_return:
                for row in frows:
                    return_row = []
                    for index in return_indices:
                        return_row.append(row[index])
                    return_data.append(return_row)

        write_perm = False
        tfile.seek(0)
        treader = csv.reader(tfile)
        for i, row in enumerate(treader):
            if i == 1:
                write_perm = True
                break
        if write_perm:
            tfile.seek(0)
            while True:
                try:
                    with open(filename, 'w', newline='') as file:
                        file.write(tfile.read())
                    if archivename:
                        shutil.copyfile(filename, archivename)
                    break
                except OSError as e:
                    print(str(e))
                    input('Ensure that the file or directory is not open or locked, then press enter to try again.')
    return return_data


def query_to_update(update_filename,
                    update_table,
                    update_targets,
                    update_metadata,
                    data,
                    where_addendums=[],
                    addendum_decorators=[],
                    archivename=None,
                    static_targets=[]):
    """The update_targets argument should be a list of strings
    wherein each is the name of a column to be updated;
    the data argument should be the return value of query_to_csv
    wherein the return_indices supplied were at least three in quantity
    whereby the first referred to the relevant emplid column,
    the second referred to the relevant adm_appl_nbr column,
    the third referenced the column of update source values,
    and the remaining optional indices cohere with the subsequent argument;
    the optional where_addendums argument should be a list of strings
    wherein the length is equal to the number of return_indices supplied
    to query_to_csv minus three and whereby each string is the name
    of an additional column to be used in the where clause;
    the optional addendum_decorators argument should be a list of
    two-string tuples wherein the length of addendum_decorators equals
    the length of where_addendums and whereby the first argument in each tuple
    is the prefix for the corresponding where_addendums argument and the second
    argument is the postfix; the optional static_targets argument should be
    a list of string two-tuples wherein the first element is a column name to be
    updated and the second element is the update value."""
    if data and len(data[0]) >= 3 and (
            not where_addendums
            or not addendum_decorators
            or len(where_addendums) == len(addendum_decorators)):
        stmt_groups = []
        excerpts = []
        while len(excerpts) < (len(where_addendums) + 3):
            excerpts.append('')
        for i, row in enumerate(data):
            if (i % 500) == 0 and i > 0:
                stmt_groups.append(excerpts.copy())
                ei = 0
                while ei < len(excerpts):
                    excerpts[ei] = ''
                    ei += 1
            excerpts[0] += ('\n  \'' + row[1] + '\', \'' + row[2] + '\',')
            excerpts[1] += ('\n  \'' + row[0] + '\',')
            excerpts[2] += ('\n  \'' + row[0] + '\', \'' + row[1] + '\',')
            for (wi, addendum) in enumerate(where_addendums):
                dvalue = prep_sql_vals(row[wi + 3])[0]
                if addendum_decorators:
                    dvalue = addendum_decorators[wi][0] + dvalue + addendum_decorators[wi][1]
                excerpts[wi + 3] += ('\n  \'' + row[1] + '\', ' + dvalue + ',')
        stmt_groups.append(excerpts)
        for row in stmt_groups:
            for i, string in enumerate(row):
                row[i] = string.rstrip(',') + '\n'
        while True:
            try:
                with open(update_filename, 'w') as file:
                    for row in stmt_groups:
                        stmt = 'UPDATE {}\nSET SCC_ROW_UPD_OPRID = {}, SCC_ROW_UPD_DTTM = {}'.format(update_table, *update_metadata)
                        if static_targets:
                            for pair in static_targets:
                                stmt = ', '.join([stmt, '{} = {}'.format(pair[0], pair[1])])
                        for target in update_targets:
                            stmt = ', '.join([stmt, '{} = DECODE(ADM_APPL_NBR, {})'.format(target, row[0])])
                        stmt += '\nWHERE EMPLID IN ({}) AND ADM_APPL_NBR = DECODE(EMPLID, {})'.format(row[1], row[2])
                        if where_addendums:
                            for wi, addendum in enumerate(where_addendums):
                                stmt += ' AND {} = DECODE(ADM_APPL_NBR, {})'.format(addendum, row[wi + 3])
                        stmt += ';\n'
                        file.write(stmt)
                if archivename:
                    shutil.copyfile(update_filename, archivename)
                break
            except OSError as e:
                print(str(e))
                input('Ensure that the file or directory is not open or locked, then press any enter to try again.')
    return None


def main():
    try:
        connop = None
        qvars = None
        root = ''
        cwd = os.getcwd()
        today = dt.datetime.today()
        if 'HOME' in os.environ:
            root = os.environ['HOME']
        else:
            root = os.environ['HOMEDRIVE'] + os.environ['HOMEPATH']
        rootop1 = os.path.join(root, 'slate_sync_vars')
        rootop2 = 'C:\\slate_sync_vars'
        if os.path.exists(rootop1):
            root = rootop1
        else:
            root = rootop2
        localdb = 'temp{}.db'.format(today.strftime('%Y%m%d%H%M%S'))
        with open(os.path.join(root, 'connect.json')) as file:
            connop = json.load(file)
        with open(os.path.join(root, 'qvars.json')) as file:
            qvars = json.load(file)
        if not (validate_keys(connop, ('oracle', 'sqlserver'))
                and validate_keys(connop['sqlserver'],
                                  ('driver',
                                   'host',
                                   'database',
                                   'user',
                                   'password'))
                and validate_keys(connop['oracle'],
                                  ('user',
                                   'password',
                                   'host',
                                   'port',
                                   'service_name'))
                and validate_keys(qvars, ('oracle',))
                and validate_keys(qvars['oracle'], ('termlb', 'termub'))):
            raise KeyError('JSON files malformed; refer to README.')
        for tdir in ['audit', 'update', '.archive']:
            testdir = os.path.join(cwd, tdir)
            if not os.path.exists(testdir):
                os.mkdir(testdir)
    except (KeyError, OSError, json.JSONDecodeError) as e:
        print(str(e))
    else:
        try:
            lconn = sqlite3.connect(localdb)

            # Setup local database
            lcur = lconn.cursor()
            lcur.execute('DROP TABLE IF EXISTS orabase')
            lcur.execute('DROP TABLE IF EXISTS oraaux')
            lcur.execute('DROP TABLE IF EXISTS mssbase')
            lcur.execute('DROP TABLE IF EXISTS oraref1')
            lcur.execute('DROP TABLE IF EXISTS oraref2')
            lcur.execute('DROP TABLE IF EXISTS oraref3')
            lconn.commit()
            lcur.execute("""CREATE TABLE orabase (
  emplid text,
  adm_appl_nbr text,
  admit_type text,
  academic_level text,
  admit_term text,
  acad_prog text,
  acad_plan text,
  prog_action text,
  prog_reason text)""")
            lcur.execute("""CREATE TABLE oraaux (
  emplid text,
  acad_career text,
  stdnt_car_nbr int,
  adm_appl_nbr text,
  appl_prog_nbr int,
  effdt text,
  effseq int,
  institution text,
  acad_prog text,
  prog_status text,
  prog_action text,
  action_dt text,
  prog_reason text,
  admit_term text,
  exp_grad_term text,
  req_term text,
  acad_load_appr text,
  campus text,
  acad_prog_dual text,
  joint_prog_appr text,
  ssr_rs_candit_nbr text,
  ssr_apt_instance int,
  ssr_yr_of_prog text,
  ssr_shift text,
  ssr_cohort_id text,
  scc_row_add_oprid text,
  scc_row_add_dttm text,
  scc_row_upd_oprid text,
  scc_row_upd_dttm text,
  Xemplid text,
  Xacad_career text,
  Xstdnt_car_nbr int,
  Xadm_appl_nbr text,
  Xappl_prog_nbr int,
  Xeffdt text,
  Xeffseq int,
  Xacad_plan text,
  Xdeclare_dt text,
  Xplan_sequence int,
  Xreq_term text,
  Xssr_apt_instance int,
  Xssr_yr_of_prog text,
  Xscc_row_add_oprid text,
  Xscc_row_add_dttm text,
  Xscc_row_upd_oprid text,
  Xscc_row_upd_dttm text)""")
            lcur.execute("""CREATE TABLE mssbase (
  emplid text,
  adm_appl_nbr text,
  admit_type,
  academic_level,
  admit_term,
  acad_prog text,
  acad_plan text,
  prog_action text,
  prog_reason text)""")
            lcur.execute('CREATE TABLE oraref1 (acad_prog text, acad_plan text)')
            lcur.execute('CREATE TABLE oraref2 (prog_action text, prog_reason text)')
            lcur.execute('CREATE TABLE oraref3 (prog_status text, prog_action text unique, rank int)')
            lconn.commit()
            lcur.execute('CREATE INDEX orab ON orabase (emplid, adm_appl_nbr)')
            lcur.execute('CREATE INDEX orax ON oraaux (emplid, adm_appl_nbr)')
            lcur.execute('CREATE INDEX ssb ON mssbase (emplid, adm_appl_nbr)')
            lcur.execute('CREATE INDEX orar1 ON oraref1 (acad_prog, acad_plan)')
            lcur.execute('CREATE INDEX orar2 ON oraref2 (prog_action, prog_reason)')
            lcur.execute('CREATE INDEX orar3 ON oraref3 (prog_status, prog_action)')
            lconn.commit()
            lcur.executemany('INSERT INTO oraref2 VALUES (?, ?)', [
                    ('APPL', ' '),
                    ('WAPP', ' '),
                    ('WADM', ' ')])
            lcur.executemany('INSERT INTO oraref3 VALUES (?, ?, ?)', [
                    ('AP', 'APPL', 0),
                    ('AP', 'DDEF', 1),
                    ('AD', 'ADMT', 2),
                    ('CN', 'DENY', 2),
                    ('AC', 'MATR', 3),
                    ('CN', 'WAPP', 3),
                    ('CN', 'WADM', 3)])
            lconn.commit()

            # Retrieve data from SQL Server database
            with pyodbc.connect(driver=connop['sqlserver']['driver'],
                                server=connop['sqlserver']['host'],
                                database=connop['sqlserver']['database'],
                                uid=connop['sqlserver']['user'],
                                pwd=connop['sqlserver']['password']) as conn:
                print(conn.getinfo(pyodbc.SQL_DRIVER_VER))
                with conn.cursor() as cur:
                    cur.execute("""select
  (select [value] from dbo.getFieldTopTable(p.[id], 'emplid')) as [EMPLID],
  (select [value] from dbo.getFieldTopTable(a.[id], 'adm_appl_nbr')) as [ADM_APPL_NBR],
  (select top 1 [value] from dbo.getFieldExportTable(a.[id], 'ug_appl_admit_type')) as [ADMIT_TYPE],
  (select top 1 [value] from dbo.getFieldExportTable(a.[id], 'ug_appl_academic_level')) as [ACADEMIC_LEVEL],
  (select top 1 [value] from dbo.getFieldExportTable(a.[id], 'appl_admit_term')) as [ADMIT_TERM],
  coalesce((select top 1 [value] from dbo.getFieldTopTable(a.[id], 'ug_appl_acad_prog_pending')), (select top 1 [value] from dbo.getFieldTopTable(a.[id], 'ug_appl_acad_prog'))) as [ACAD_PROG],
  coalesce((select top 1 [value] from dbo.getFieldExportTable(a.[id], 'ug_appl_acad_plan_pending')), (select top 1 [value] from dbo.getFieldExportTable(a.[id], 'ug_appl_acad_plan'))) as [ACAD_PLAN],
  (select top 1 [value] from dbo.getFieldExportTable(a.[id], 'prog_action')) as [PROG_ACTION],
  (select top 1 [value] from dbo.getFieldExportTable(a.[id], 'prog_reason')) as [PROG_REASON]
from [application] a
inner join [person] p on p.[id] = a.[person]
inner join [lookup.round] lr on lr.[id] = a.[round]
where p.[id] not in (select [record] from [tag] where ([tag] in ('test')))
and isnull(lr.[key], '') = 'UG'
and a.[submitted] is not null
and lr.[active] = 1
order by 1, 2""")
                    fc = 0
                    while True:
                        rows = cur.fetchmany(500)
                        if not rows:
                            print(f'\nFetched and inserted {cur.rowcount} total rows.\n\n')
                            break
                        lcur.executemany('INSERT INTO mssbase VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)', rows)
                        lconn.commit()
                        print(f'Fetched and inserted from row {fc*500 + 1}...')
                        fc += 1
                    lcur.execute("UPDATE mssbase SET prog_reason = ' ' WHERE prog_reason IS NULL")
                    lconn.commit()
            conn.close()

            # Retrieve data from Oracle database
            with cxo.connect(connop['oracle']['user'],
                             connop['oracle']['password'],
                             cxo.makedsn(connop['oracle']['host'],
                                         connop['oracle']['port'],
                                         service_name=connop['oracle']['service_name'])) as conn:
                print(conn.version)
                with conn.cursor() as cur:
                    cur.execute("""SELECT
  A.EMPLID,
  A.ADM_APPL_NBR,
  A.ADMIT_TYPE,
  A.ACADEMIC_LEVEL,
  A.ADMIT_TERM,
  A.ACAD_PROG,
  A.ACAD_PLAN,
  A.PROG_ACTION,
  A.PROG_REASON
FROM PS_L_ADM_PROG_VW A
WHERE A.ADMIT_TERM BETWEEN :termlb AND :termub
AND A.ACAD_CAREER = 'UGRD'
ORDER BY A.EMPLID, A.ADM_APPL_NBR""", qvars['oracle'])
                    fc = 0
                    while True:
                        rows = cur.fetchmany(500)
                        if not rows:
                            print(f'\nFetched and inserted {cur.rowcount} total rows.\n\n')
                            break
                        lcur.executemany('INSERT INTO orabase VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)', rows)
                        lconn.commit()
                        print(f'Fetched and inserted from row {fc*500 + 1}...')
                        fc += 1
                    cur.execute("""SELECT
  A.EMPLID,
  A.ACAD_CAREER,
  A.STDNT_CAR_NBR,
  A.ADM_APPL_NBR,
  A.APPL_PROG_NBR,
  TO_CHAR(A.EFFDT, 'YYYY-MM-DD'),
  A.EFFSEQ,
  A.INSTITUTION,
  A.ACAD_PROG,
  A.PROG_STATUS,
  A.PROG_ACTION,
  TO_CHAR(A.ACTION_DT, 'YYYY-MM-DD'),
  A.PROG_REASON,
  A.ADMIT_TERM,
  A.EXP_GRAD_TERM,
  A.REQ_TERM,
  A.ACAD_LOAD_APPR,
  A.CAMPUS,
  A.ACAD_PROG_DUAL,
  A.JOINT_PROG_APPR,
  A.SSR_RS_CANDIT_NBR,
  A.SSR_APT_INSTANCE,
  A.SSR_YR_OF_PROG,
  A.SSR_SHIFT,
  A.SSR_COHORT_ID,
  A.SCC_ROW_ADD_OPRID,
  A.SCC_ROW_ADD_DTTM,
  A.SCC_ROW_UPD_OPRID,
  A.SCC_ROW_UPD_DTTM,
  B.EMPLID,
  B.ACAD_CAREER,
  B.STDNT_CAR_NBR,
  B.ADM_APPL_NBR,
  B.APPL_PROG_NBR,
  TO_CHAR(B.EFFDT, 'YYYY-MM-DD'),
  B.EFFSEQ,
  B.ACAD_PLAN,
  TO_CHAR(B.DECLARE_DT, 'YYYY-MM-DD'),
  B.PLAN_SEQUENCE,
  B.REQ_TERM,
  B.SSR_APT_INSTANCE,
  B.SSR_YR_OF_PROG,
  B.SCC_ROW_ADD_OPRID,
  B.SCC_ROW_ADD_DTTM,
  B.SCC_ROW_UPD_OPRID,
  B.SCC_ROW_UPD_DTTM
FROM PS_ADM_APPL_PROG A
INNER JOIN PS_ADM_APPL_PLAN B ON A.EMPLID = B.EMPLID AND A.ACAD_CAREER = B.ACAD_CAREER AND A.STDNT_CAR_NBR = B.STDNT_CAR_NBR AND A.ADM_APPL_NBR = B.ADM_APPL_NBR AND A.APPL_PROG_NBR = B.APPL_PROG_NBR AND A.EFFDT = B.EFFDT AND A.EFFSEQ = B.EFFSEQ AND B.PLAN_SEQUENCE = 1
WHERE A.ADMIT_TERM BETWEEN :termlb AND :termub
AND A.ACAD_CAREER = 'UGRD'
AND A.EFFDT = (
  SELECT MAX(A_ED.EFFDT)
  FROM PS_ADM_APPL_PROG A_ED
  WHERE A.EMPLID = A_ED.EMPLID
  AND A.ACAD_CAREER = A_ED.ACAD_CAREER
  AND A.STDNT_CAR_NBR = A_ED.STDNT_CAR_NBR
  AND A.ADM_APPL_NBR = A_ED.ADM_APPL_NBR
  AND A.APPL_PROG_NBR = A_ED.APPL_PROG_NBR
) AND A.EFFSEQ = (
  SELECT MAX(A_ED.EFFSEQ)
  FROM PS_ADM_APPL_PROG A_ED
  WHERE A.EMPLID = A_ED.EMPLID
  AND A.ACAD_CAREER = A_ED.ACAD_CAREER
  AND A.STDNT_CAR_NBR = A_ED.STDNT_CAR_NBR
  AND A.ADM_APPL_NBR = A_ED.ADM_APPL_NBR
  AND A.APPL_PROG_NBR = A_ED.APPL_PROG_NBR
  AND A.EFFDT = A_ED.EFFDT
)
ORDER BY A.EMPLID, A.ADM_APPL_NBR""", qvars['oracle'])
                    fc = 0
                    while True:
                        rows = cur.fetchmany(500)
                        if not rows:
                            print(f'\nFetched and inserted {cur.rowcount} total rows.\n\n')
                            break
                        lcur.executemany('INSERT INTO oraaux VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', rows)
                        lconn.commit()
                        print(f'Fetched and inserted from row {fc*500 + 1}...')
                        fc += 1
                    cur.execute("""SELECT DISTINCT A.ACAD_PROG, B.ACAD_PLAN
FROM PS_ACAD_PROG_TBL A
INNER JOIN PS_ACAD_PLAN_TBL B ON A.ACAD_PROG = B.ACAD_PROG AND A.EFF_STATUS = B.EFF_STATUS AND B.EFFDT = (
  SELECT MAX(B_ED.EFFDT)
  FROM PS_ACAD_PLAN_TBL B_ED
  WHERE B.ACAD_PLAN = B_ED.ACAD_PLAN
)
WHERE A.EFF_STATUS = 'A'
AND A.ACAD_CAREER = 'UGRD'
AND A.EFFDT = (
  SELECT MAX(A_ED.EFFDT)
  FROM PS_ACAD_PROG_TBL A_ED
  WHERE A.ACAD_PROG = A_ED.ACAD_PROG
)
AND B.ACAD_PLAN_TYPE <> 'MIN'
UNION
SELECT DISTINCT A.ACAD_PROG, A.ACAD_PLAN
FROM PS_ACAD_PROG_TBL A
WHERE A.EFF_STATUS = 'A'
AND A.ACAD_CAREER = 'UGRD'
AND A.EFFDT = (
  SELECT MAX(A_ED.EFFDT)
  FROM PS_ACAD_PROG_TBL A_ED
  WHERE A.ACAD_PROG = A_ED.ACAD_PROG
)
AND A.ACAD_PLAN <> ' '
UNION
SELECT DISTINCT A.ACAD_PROG, B.ACAD_PLAN
FROM PS_ACAD_PROG_TBL A
INNER JOIN PS_ACAD_PLAN_TBL B ON A.ACAD_CAREER = B.ACAD_CAREER AND A.EFF_STATUS = B.EFF_STATUS AND B.EFFDT = (
  SELECT MAX(B_ED.EFFDT)
  FROM PS_ACAD_PLAN_TBL B_ED
  WHERE B.ACAD_PLAN = B_ED.ACAD_PLAN
)
WHERE A.EFF_STATUS = 'A'
AND A.ACAD_CAREER = 'UGRD'
AND A.EFFDT = (
  SELECT MAX(A_ED.EFFDT)
  FROM PS_ACAD_PROG_TBL A_ED
  WHERE A.ACAD_PROG = A_ED.ACAD_PROG
)
AND B.ACAD_PROG = ' '
AND B.ACAD_PLAN_TYPE <> 'MIN'
ORDER BY 1, 2""")
                    fc = 0
                    while True:
                        rows = cur.fetchmany(500)
                        if not rows:
                            print(f'\nFetched and inserted {cur.rowcount} total rows.\n\n')
                            break
                        lcur.executemany('INSERT INTO oraref1 VALUES (?, ?)', rows)
                        lconn.commit()
                        print(f'Fetched and inserted from row {fc*500 + 1}...')
                        fc += 1
                    cur.execute("""SELECT A.PROG_ACTION, B.PROG_REASON
FROM PS_ADM_ACTION_TBL A
INNER JOIN PS_PROG_RSN_TBL B ON A.PROG_ACTION = B.PROG_ACTION AND A.EFF_STATUS = B.EFF_STATUS AND B.EFFDT = (
  SELECT MAX(B_ED.EFFDT)
  FROM PS_PROG_RSN_TBL B_ED
  WHERE B.PROG_ACTION = B_ED.PROG_ACTION
  AND B.PROG_REASON = B_ED.PROG_REASON
)
ORDER BY 1, 2""")
                    fc = 0
                    while True:
                        rows = cur.fetchmany(500)
                        if not rows:
                            print(f'\nFetched and inserted {cur.rowcount} total rows.\n\n')
                            break
                        lcur.executemany('INSERT INTO oraref2 VALUES (?, ?)', rows)
                        lconn.commit()
                        print(f'Fetched and inserted from row {fc*500 + 1}...')
                        fc += 1

            # Query local database
            row_metauser = '\'slate_sync - ' + connop['oracle']['user'].upper() + '\''
            row_metadttm = 'SYSDATE'
            row_metadata = (row_metauser, row_metadttm)
            ippc = """SELECT *
FROM mssbase as msb
INNER JOIN orabase as orb on msb.emplid = orb.emplid and msb.adm_appl_nbr = orb.adm_appl_nbr
WHERE NOT EXISTS (
  SELECT *
  FROM oraref1 as orr1
  WHERE msb.acad_prog = orr1.acad_prog
  AND msb.acad_plan = orr1.acad_plan
)
ORDER BY 1, 2"""
            iarc = """SELECT *
FROM mssbase as msb
INNER JOIN orabase as orb on msb.emplid = orb.emplid and msb.adm_appl_nbr = orb.adm_appl_nbr
WHERE NOT EXISTS (
  SELECT *
  FROM oraref2 as orr2
  WHERE msb.prog_action = orr2.prog_action
  AND msb.prog_reason = orr2.prog_reason
)
ORDER BY 1, 2"""
            iau = """SELECT msb.*, orb.*
FROM mssbase as msb
INNER JOIN oraref3 as msborr3 on msb.prog_action = msborr3.prog_action
INNER JOIN orabase as orb on msb.emplid = orb.emplid and msb.adm_appl_nbr = orb.adm_appl_nbr
INNER JOIN oraref3 as orborr3 on orb.prog_action = orborr3.prog_action
WHERE msb.prog_action != orb.prog_action
AND msborr3.rank <= orborr3.rank
ORDER BY 1, 2"""
            cla = """SELECT *
FROM mssbase as msb
INNER JOIN orabase as orb on msb.emplid = orb.emplid and msb.adm_appl_nbr = orb.adm_appl_nbr
INNER JOIN oraref3 as orr3 on orb.prog_action = orr3.prog_action
WHERE orr3.rank = 3
AND (
  orb.admit_type != msb.admit_type
  OR orb.academic_level != msb.academic_level
  OR orb.admit_term != msb.admit_term
  OR orb.acad_prog != msb.acad_prog
  OR orb.acad_plan != msb.acad_plan
  OR orb.prog_action != msb.prog_action
  OR orb.prog_reason != msb.prog_reason
)
ORDER BY 1, 2"""
            ui = """
  SELECT umsb.adm_appl_nbr
  FROM mssbase as umsb
  INNER JOIN orabase as uorb on umsb.emplid = uorb.emplid and umsb.adm_appl_nbr = uorb.adm_appl_nbr
  WHERE NOT EXISTS (
    SELECT *
    FROM oraref1 as uorr1
    WHERE umsb.acad_prog = uorr1.acad_prog
    AND umsb.acad_plan = uorr1.acad_plan
  )
  UNION
  SELECT umsb.adm_appl_nbr
  FROM mssbase as umsb
  INNER JOIN orabase as uorb on umsb.emplid = uorb.emplid and umsb.adm_appl_nbr = uorb.adm_appl_nbr
  WHERE NOT EXISTS (
    SELECT *
    FROM oraref2 as uorr2
    WHERE umsb.prog_action = uorr2.prog_action
    AND umsb.prog_reason = uorr2.prog_reason
  )
  UNION
  SELECT umsb.adm_appl_nbr
  FROM mssbase as umsb
  INNER JOIN oraref3 as umsborr3 on umsb.prog_action = umsborr3.prog_action
  INNER JOIN orabase as uorb on umsb.emplid = uorb.emplid and umsb.adm_appl_nbr = uorb.adm_appl_nbr
  INNER JOIN oraref3 as uorborr3 on uorb.prog_action = uorborr3.prog_action
  WHERE umsb.prog_action != uorb.prog_action
  AND umsborr3.rank <= uorborr3.rank
  UNION
  SELECT umsb.adm_appl_nbr
  FROM mssbase as umsb
  INNER JOIN orabase as uorb on umsb.emplid = uorb.emplid and umsb.adm_appl_nbr = uorb.adm_appl_nbr
  INNER JOIN oraref3 as uorr3 on uorb.prog_action = uorr3.prog_action
  WHERE uorr3.rank = 3
  AND (
    uorb.admit_type != umsb.admit_type
    OR uorb.academic_level != umsb.academic_level
    OR uorb.admit_term != umsb.admit_term
    OR uorb.acad_prog != umsb.acad_prog
    OR uorb.acad_plan != umsb.acad_plan
    OR uorb.prog_action != umsb.prog_action
    OR uorb.prog_reason != umsb.prog_reason
  )
"""

            lcur.execute(ippc)
            query_to_csv(os.path.join(cwd, 'audit', 'INVALID_PP_COMBO.csv'), lcur)
            lcur.execute(iarc)
            query_to_csv(os.path.join(cwd, 'audit', 'INVALID_AR_COMBO.csv'), lcur)
            lcur.execute(iau)
            query_to_csv(os.path.join(cwd, 'audit', 'INVALID_ACTION_UPDATE.csv'), lcur)
            lcur.execute(cla)
            query_to_csv(os.path.join(cwd, 'audit', 'CHANGES_TO_LOCKED_APPLICATIONS.csv'), lcur)

            lcur.execute("""SELECT *
FROM mssbase as msb
INNER JOIN orabase as orb on msb.emplid = orb.emplid and msb.adm_appl_nbr = orb.adm_appl_nbr
WHERE msb.adm_appl_nbr NOT IN (""" + ui + """) AND msb.admit_type != orb.admit_type
AND orb.admit_type != 'XRE'
ORDER BY 1, 2""")
            ldata = query_to_csv(os.path.join(cwd, 'update', 'TYPE_CHANGE.csv'),
                                 lcur,
                                 [0, 1, 2],
                                 os.path.join(cwd, '.archive', 'TYPE_CHANGE_{}.csv'.format(today.strftime('%Y%m%d'))))
            query_to_update(os.path.join(cwd, 'update', 'update_type.txt'),
                            'PS_ADM_APPL_DATA',
                            ['ADMIT_TYPE'],
                            row_metadata,
                            ldata,
                            archivename=os.path.join(cwd, '.archive', 'update_type_{}.txt'.format(today.strftime('%Y%m%d'))),
                            static_targets=[('ADM_UPDATED_DT', 'TRUNC(SYSDATE)'), ('ADM_UPDATED_BY', row_metauser)])
            lcur.execute("""SELECT *
FROM mssbase as msb
INNER JOIN orabase as orb on msb.emplid = orb.emplid and msb.adm_appl_nbr = orb.adm_appl_nbr
WHERE msb.adm_appl_nbr NOT IN (""" + ui + """) AND msb.academic_level != orb.academic_level
ORDER BY 1, 2""")
            ldata = query_to_csv(os.path.join(cwd, 'update', 'LEVEL_CHANGE.csv'),
                                 lcur,
                                 [0, 1, 3],
                                 os.path.join(cwd, '.archive', 'LEVEL_CHANGE_{}.csv'.format(today.strftime('%Y%m%d'))))
            query_to_update(os.path.join(cwd, 'update', 'update_level.txt'),
                            'PS_ADM_APPL_DATA',
                            ['ACADEMIC_LEVEL'],
                            row_metadata,
                            ldata,
                            archivename=os.path.join(cwd, '.archive', 'update_level_{}.txt'.format(today.strftime('%Y%m%d'))),
                            static_targets=[('ADM_UPDATED_DT', 'TRUNC(SYSDATE)'), ('ADM_UPDATED_BY', row_metauser)])
            lcur.execute("""SELECT *
FROM mssbase as msb
INNER JOIN orabase as orb on msb.emplid = orb.emplid and msb.adm_appl_nbr = orb.adm_appl_nbr
WHERE msb.adm_appl_nbr NOT IN (""" + ui + """) AND msb.admit_term != orb.admit_term
ORDER BY 1, 2""")
            ldata = query_to_csv(os.path.join(cwd, 'update', 'TERM_CHANGE.csv'),
                                 lcur,
                                 [0, 1, 4],
                                 os.path.join(cwd, '.archive', 'TERM_CHANGE_{}.csv'.format(today.strftime('%Y%m%d'))))
            query_to_update(os.path.join(cwd, 'update', 'update_term_prog.txt'),
                            'PS_ADM_APPL_PROG',
                            ['ADMIT_TERM', 'REQ_TERM'],
                            row_metadata,
                            ldata,
                            archivename=os.path.join(cwd, '.archive', 'update_term_prog_{}.txt'.format(today.strftime('%Y%m%d'))))
            query_to_update(os.path.join(cwd, 'update', 'update_term_plan.txt'),
                            'PS_ADM_APPL_PLAN',
                            ['REQ_TERM'],
                            row_metadata,
                            ldata,
                            archivename=os.path.join(cwd, '.archive', 'update_term_plan_{}.txt'.format(today.strftime('%Y%m%d'))))
            lcur.execute("""SELECT *
FROM mssbase as msb
INNER JOIN orabase as orb on msb.emplid = orb.emplid and msb.adm_appl_nbr = orb.adm_appl_nbr
WHERE msb.adm_appl_nbr NOT IN (""" + ui + """) AND msb.acad_prog != orb.acad_prog
ORDER BY 1, 2""")
            ldata = query_to_csv(os.path.join(cwd, 'update', 'PROG_CHANGE.csv'),
                                 lcur,
                                 [0, 1, 5],
                                 os.path.join(cwd, '.archive', 'PROG_CHANGE_{}.csv'.format(today.strftime('%Y%m%d'))))
            query_to_update(os.path.join(cwd, 'update', 'update_prog.txt'),
                            'PS_ADM_APPL_PROG',
                            ['ACAD_PROG'],
                            row_metadata,
                            ldata,
                            archivename=os.path.join(cwd, '.archive', 'update_prog_{}.txt'.format(today.strftime('%Y%m%d'))))
            lcur.execute("""SELECT *
FROM mssbase as msb
INNER JOIN orabase as orb on msb.emplid = orb.emplid and msb.adm_appl_nbr = orb.adm_appl_nbr
WHERE msb.adm_appl_nbr NOT IN (""" + ui + """) AND msb.acad_plan != orb.acad_plan
ORDER BY 1, 2""")
            ldata = query_to_csv(os.path.join(cwd, 'update', 'PLAN_CHANGE.csv'),
                                 lcur,
                                 [0, 1, 6],
                                 os.path.join(cwd, '.archive', 'PLAN_CHANGE_{}.csv'.format(today.strftime('%Y%m%d'))))
            query_to_update(os.path.join(cwd, 'update', 'update_plan.txt'),
                            'PS_ADM_APPL_PLAN',
                            ['ACAD_PLAN'],
                            row_metadata,
                            ldata,
                            archivename=os.path.join(cwd, '.archive', 'update_plan_{}.txt'.format(today.strftime('%Y%m%d'))))
            lcur.execute("""SELECT
  msb.*,
  orb.*,
  '' as [BREAK],
  orx.emplid,
  orx.acad_career,
  orx.stdnt_car_nbr,
  orx.adm_appl_nbr,
  orx.appl_prog_nbr,
  orx.effdt,
  orx.effseq,
  orx.institution,
  orx.acad_prog,
  orx.prog_status,
  orx.prog_action,
  orx.action_dt,
  orx.prog_reason,
  orx.admit_term,
  orx.exp_grad_term,
  orx.req_term,
  orx.acad_load_appr,
  orx.campus,
  orx.acad_prog_dual,
  orx.joint_prog_appr,
  orx.ssr_rs_candit_nbr,
  orx.ssr_apt_instance,
  orx.ssr_yr_of_prog,
  orx.ssr_shift,
  orx.ssr_cohort_id,
  orx.scc_row_add_oprid,
  orx.scc_row_add_dttm,
  orx.scc_row_upd_oprid,
  orx.scc_row_upd_dttm
FROM mssbase as msb
INNER JOIN orabase as orb on msb.emplid = orb.emplid and msb.adm_appl_nbr = orb.adm_appl_nbr
INNER JOIN oraaux as orx on orb.emplid = orx.emplid and orb.adm_appl_nbr = orx.adm_appl_nbr
WHERE msb.adm_appl_nbr NOT IN (""" + ui + """) AND msb.prog_action = orb.prog_action
AND msb.prog_reason != orb.prog_reason
ORDER BY 1, 2""")
            ldata = query_to_csv(os.path.join(cwd, 'update', 'REASON_CHANGE.csv'),
                                 lcur,
                                 [0, 1, 8, 24, 25],
                                 os.path.join(cwd, '.archive', 'REASON_CHANGE_{}.csv'.format(today.strftime('%Y%m%d'))))
            query_to_update(os.path.join(cwd, 'update', 'update_reason.txt'),
                            'PS_ADM_APPL_PROG',
                            ['PROG_REASON'],
                            row_metadata,
                            ldata,
                            ['EFFDT', 'EFFSEQ'],
                            [('TO_DATE(', ', \'YYYY-MM-DD\')'), ('', '')],
                            os.path.join(cwd, '.archive', 'update_reason_{}.txt'.format(today.strftime('%Y%m%d'))))
            lcur.execute("""SELECT
  msb.*,
  orb.*,
  '' as [BREAK],
  orx.emplid,
  orx.acad_career,
  orx.stdnt_car_nbr,
  orx.adm_appl_nbr,
  orx.appl_prog_nbr,
  orx.effdt,
  orx.effseq,
  orx.institution,
  msb.acad_prog,
  orr3.prog_status,
  msb.prog_action,
  '',
  msb.prog_reason,
  msb.admit_term,
  orx.exp_grad_term,
  msb.admit_term,
  orx.acad_load_appr,
  orx.campus,
  orx.acad_prog_dual,
  orx.joint_prog_appr,
  orx.ssr_rs_candit_nbr,
  orx.ssr_apt_instance,
  orx.ssr_yr_of_prog,
  orx.ssr_shift,
  orx.ssr_cohort_id,
  '' as [BREAK],
  orx.Xemplid,
  orx.Xacad_career,
  orx.Xstdnt_car_nbr,
  orx.Xadm_appl_nbr,
  orx.Xappl_prog_nbr,
  orx.Xeffdt,
  orx.Xeffseq,
  msb.acad_plan,
  orx.Xdeclare_dt,
  orx.Xplan_sequence,
  msb.admit_term,
  orx.Xssr_apt_instance,
  orx.Xssr_yr_of_prog
FROM mssbase as msb
INNER JOIN oraref3 as orr3 on msb.prog_action = orr3.prog_action
INNER JOIN orabase as orb on msb.emplid = orb.emplid and msb.adm_appl_nbr = orb.adm_appl_nbr
INNER JOIN oraaux as orx on orb.emplid = orx.emplid and orb.adm_appl_nbr = orx.adm_appl_nbr
WHERE msb.adm_appl_nbr NOT IN (""" + ui + """) AND msb.prog_action != orb.prog_action
AND msb.admit_term is not null
AND msb.acad_prog is not null
AND msb.acad_plan is not null
ORDER BY 1, 2""")
            ldata = query_to_csv(os.path.join(cwd, 'update', 'ACTION_CHANGE.csv'),
                                 lcur,
                                 range(19, 58),
                                 os.path.join(cwd, '.archive', 'ACTION_CHANGE_{}.csv'.format(today.strftime('%Y%m%d'))))
            if ldata:
                stmt_groups = []
                excerpt = ''
                for i, row in enumerate(ldata):
                    if (i % 250) == 0 and i > 0:
                        stmt_groups.append(excerpt)
                        excerpt = ''
                    effseq = (str(row[6] + 1) if dt.datetime.strptime(row[5], '%Y-%m-%d').date() == today.date() else '1')
                    excerpt += '  INTO PS_ADM_APPL_PROG VALUES ({})\n'.format(
                            ', '.join(prep_sql_vals(*row[0:5]))
                            + ', TRUNC(SYSDATE), {}, '.format(effseq)
                            + ', '.join(prep_sql_vals(*row[7:11]))
                            + ', TRUNC(SYSDATE), '
                            + ', '.join(prep_sql_vals(*row[12:25]))
                            + ', '
                            + ', '.join([*row_metadata, *row_metadata]))
                    excerpt += '  INTO PS_ADM_APPL_PLAN VALUES ({})\n'.format(
                            ', '.join(prep_sql_vals(*row[26:31]))
                            + ', TRUNC(SYSDATE), {}, '.format(effseq)
                            + ', '.join(prep_sql_vals(row[33]))
                            + ', TO_DATE({}, \'YYYY-MM-DD\'), '.format(*prep_sql_vals(row[34]))
                            + ', '.join(prep_sql_vals(*row[35:]))
                            + ', '
                            + ', '.join([*row_metadata, *row_metadata]))
                stmt_groups.append(excerpt)
                while True:
                    try:
                        with open(os.path.join(cwd, 'update', 'insert_action.txt'), 'w') as file:
                            for row in stmt_groups:
                                file.write('INSERT ALL\n{}SELECT * FROM dual;\n'.format(row))
                        shutil.copyfile(os.path.join(cwd, 'update', 'insert_action.txt'),
                                        os.path.join(cwd, '.archive', 'insert_action_{}.txt'.format(today.strftime('%Y%m%d'))))
                        break
                    except OSError as e:
                        print(str(e))
                        input('Ensure that the file or directory is not open or locked, then press any enter to try again.')

            # Cleanup local database
            lcur.execute('DROP TABLE IF EXISTS orabase')
            lcur.execute('DROP TABLE IF EXISTS oraaux')
            lcur.execute('DROP TABLE IF EXISTS mssbase')
            lcur.execute('DROP TABLE IF EXISTS oraref1')
            lcur.execute('DROP TABLE IF EXISTS oraref2')
            lcur.execute('DROP TABLE IF EXISTS oraref3')
            lconn.commit()
        except (pyodbc.DatabaseError) as e:
            conn.close()
            print(str(e))
        except (OSError, cxo.Error, sqlite3.DatabaseError) as e:
            print(str(e))
        finally:
            lconn.rollback()
            lcur.close()
            lconn.close()
            os.remove(localdb)
    finally:
        input('Press enter to finish...')


if __name__ == '__main__':
    main()
