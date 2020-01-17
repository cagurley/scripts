# -*- coding: utf-8 -*-
"""
Created on Fri Nov  8 10:58:37 2019

@author: cagurl01
"""

import csv
import cx_Oracle as cxo
import json
import pyodbc
import sqlite3


def validate_keys(srcdict, keys=tuple()):
    for key in keys:
        if key not in srcdict:
            return False
    return True


def query_to_csv(filename, cursor):
    with open(filename, 'w', newline='') as file:
        writer = csv.writer(file)
        header = []
        for row in cursor.description:
            if len(row) > 0:
                header.append(row[0])
        writer.writerow(header)
        counter = 0
        while True:
            frows = cursor.fetchmany(500)
            if not frows:
                print(f'\nFetched and wrote {cursor.rowcount} total rows.\n\n')
                break
            print(f'Fetched and wrote from row {counter*500 + 1}...')
            counter += 1
            writer.writerows(frows)


try:
    connop = None
    qvars = None
    with open('connect.json') as file:
        connop = json.load(file)
    with open('qvars.json') as file:
        qvars = json.load(file)
    if (validate_keys(connop, ('oracle', 'sqlserver'))
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
            and 'oracle' in qvars
            and validate_keys(qvars['oracle'], ('termlb', 'termub'))):
        lconn = sqlite3.connect('temp.db')

        # Setup local database
        lcur = lconn.cursor()
        lcur.execute('DROP TABLE IF EXISTS orabase')
        lcur.execute('DROP TABLE IF EXISTS mssbase')
        lcur.execute('DROP TABLE IF EXISTS oraref1')
        lconn.commit()
        lcur.execute("""CREATE TABLE orabase (
  emplid text,
  adm_appl_nbr text,
  admit_type,
  academic_level,
  admit_term,
  acad_prog text,
  acad_plan text,
  prog_action text,
  prog_reason text)""")
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
        lcur.execute('CREATE TABLE oraref2 (prog_action text, rank int)')
        lconn.commit()
        lcur.execute('CREATE INDEX orab ON orabase (emplid, adm_appl_nbr)')
        lcur.execute('CREATE INDEX ssb ON mssbase (emplid, adm_appl_nbr)')
        lcur.execute('CREATE INDEX orar1 ON oraref1 (acad_prog, acad_plan)')
        lcur.execute('CREATE INDEX orar2 ON oraref2 (prog_action)')
        lconn.commit()
        lcur.executemany('INSERT INTO oraref2 VALUES (?, ?)', [
                ('APPL', 0),
                ('DDEF', 1),
                ('ADMT', 2),
                ('DENY', 2),
                ('MATR', 3),
                ('WAPP', 3),
                ('WADM', 3)])
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
left outer join [lookup.round] r on r.[id] = a.[round]
where p.[id] not in (select [record] from [tag] where ([tag] in ('test')))
and isnull(r.[key], '') = 'UG'
and a.[submitted] is not null
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

        # Query local database
        lcur.execute("""SELECT *
FROM mssbase as msb
INNER JOIN orabase as orb on msb.emplid = orb.emplid and msb.adm_appl_nbr = orb.adm_appl_nbr
WHERE NOT EXISTS (
  SELECT *
  FROM oraref1 as orr1
  WHERE msb.acad_prog = orr1.acad_prog
  AND msb.acad_plan = orr1.acad_plan
)
ORDER BY 1, 2""")
        query_to_csv('INVALID_PP_COMBO.csv', lcur)
        lcur.execute("""SELECT *
FROM mssbase as msb
INNER JOIN orabase as orb on msb.emplid = orb.emplid and msb.adm_appl_nbr = orb.adm_appl_nbr
WHERE EXISTS (
  SELECT *
  FROM oraref1 as orr1
  WHERE msb.acad_prog = orr1.acad_prog
  AND msb.acad_plan = orr1.acad_plan
) AND msb.acad_prog != orb.acad_prog
ORDER BY 1, 2""")
        query_to_csv('BAD_PROG.csv', lcur)
        lcur.execute("""SELECT *
FROM mssbase as msb
INNER JOIN orabase as orb on msb.emplid = orb.emplid and msb.adm_appl_nbr = orb.adm_appl_nbr
WHERE EXISTS (
  SELECT *
  FROM oraref as orr1
  WHERE msb.acad_prog = orr1.acad_prog
  AND msb.acad_plan = orr1.acad_plan
) AND msb.acad_plan != orb.acad_plan
ORDER BY 1, 2""")
        query_to_csv('BAD_PLAN.csv', lcur)
        lcur.execute("""SELECT *
FROM mssbase as msb
INNER JOIN orabase as orb on msb.emplid = orb.emplid and msb.adm_appl_nbr = orb.adm_appl_nbr
WHERE msb.admit_type != orb.admit_type
ORDER BY 1, 2""")
        query_to_csv('BAD_TYPE.csv', lcur)
        lcur.execute("""SELECT *
FROM mssbase as msb
INNER JOIN orabase as orb on msb.emplid = orb.emplid and msb.adm_appl_nbr = orb.adm_appl_nbr
WHERE msb.admit_term != orb.admit_term
ORDER BY 1, 2""")
        query_to_csv('BAD_TERM.csv', lcur)
    else:
        print('Missing keys in JSON files; check for proper formation.')
except (OSError, json.JSONDecodeError, cxo.Error, pyodbc.DatabaseError, sqlite3.DatabaseError) as e:
    print(str(e))
finally:
    lconn.rollback()
    lcur.close()
    lconn.close()
