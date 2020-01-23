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


def query_to_csv(filename, cursor, return_indices=None):
    with open(filename, 'w', newline='') as file:
        writer = csv.writer(file)
        header = []
        should_return = False
        for row in cursor.description:
            if len(row) > 0:
                header.append(row[0])
        writer.writerow(header)
        if return_indices:
            for index in return_indices:
                if len(header) >= index:
                    should_return = True

        counter = 0
        return_data = []
        while True:
            frows = cursor.fetchmany(500)
            if not frows:
                print(f'\nFetched and wrote {cursor.rowcount} total rows.\n\n')
                break
            print(f'Fetched and wrote from row {counter*500 + 1}...')
            counter += 1
            writer.writerows(frows)
            if should_return:
                for row in frows:
                    return_row = []
                    for index in return_indices:
                        return_row.append(row[index])
                    return_data.append(return_row)
        return return_data


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
        lcur.execute('DROP TABLE IF EXISTS oraref2')
        lcur.execute('DROP TABLE IF EXISTS oraref3')
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
        lcur.execute('CREATE TABLE oraref2 (prog_action text, prog_reason text)')
        lcur.execute('CREATE TABLE oraref3 (prog_action text unique, rank int)')
        lconn.commit()
        lcur.execute('CREATE INDEX orab ON orabase (emplid, adm_appl_nbr)')
        lcur.execute('CREATE INDEX ssb ON mssbase (emplid, adm_appl_nbr)')
        lcur.execute('CREATE INDEX orar1 ON oraref1 (acad_prog, acad_plan)')
        lcur.execute('CREATE INDEX orar2 ON oraref2 (prog_action, prog_reason)')
        lcur.execute('CREATE INDEX orar3 ON oraref3 (prog_action)')
        lconn.commit()
        lcur.executemany('INSERT INTO oraref2 VALUES (?, ?)', [
                ('APPL', ' '),
                ('WAPP', ' '),
                ('WADM', ' ')])
        lcur.executemany('INSERT INTO oraref3 VALUES (?, ?)', [
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
                lcur.execute("UPDATE mssbase SET prog_reason = ' ' WHERE prog_reason IS NULL")
                lconn.commit()

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
        ippc = """SELECT *
FROM mssbase as msb
INNER JOIN orabase as orb on msb.emplid = orb.emplid and msb.adm_appl_nbr = orb.adm_appl_nbr
WHERE NOT EXISTS (
  SELECT *
  FROM oraref1 as orr1
  WHERE msb.acad_prog = orr1.acad_prog
  AND msb.acad_plan = orr1.acad_plan
)"""
        iarc = """SELECT *
FROM mssbase as msb
INNER JOIN orabase as orb on msb.emplid = orb.emplid and msb.adm_appl_nbr = orb.adm_appl_nbr
WHERE NOT EXISTS (
  SELECT *
  FROM oraref2 as orr2
  WHERE msb.prog_action = orr2.prog_action
  AND msb.prog_reason = orr2.prog_reason
)"""
        iau = """SELECT msb.*, orb.*
FROM mssbase as msb
INNER JOIN oraref3 as msborr3 on msb.prog_action = msborr3.prog_action
INNER JOIN orabase as orb on msb.emplid = orb.emplid and msb.adm_appl_nbr = orb.adm_appl_nbr
INNER JOIN oraref3 as orborr3 on orb.prog_action = orborr3.prog_action
WHERE msb.prog_action != orb.prog_action
AND msborr3.rank <= orborr3.rank"""
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
"""

        lcur.execute(ippc + '\nORDER BY 1, 2')
        query_to_csv('INVALID_PP_COMBO.csv', lcur)
        lcur.execute(iarc + '\nORDER BY 1, 2')
        query_to_csv('INVALID_AR_COMBO.csv', lcur)
        lcur.execute(iau + '\nORDER BY 1, 2')
        query_to_csv('INVALID_ACTION_UPDATE.csv', lcur)

        lcur.execute("""SELECT *
FROM mssbase as msb
INNER JOIN orabase as orb on msb.emplid = orb.emplid and msb.adm_appl_nbr = orb.adm_appl_nbr
WHERE msb.adm_appl_nbr NOT IN (""" + ui + """) AND msb.admit_type != orb.admit_type
ORDER BY 1, 2""")
        ldata = query_to_csv('BAD_TYPE.csv', lcur, [0, 1, 2])
        if ldata:
            excerpts = ['', '', '']
            for row in ldata:
                excerpts[0] += ('\n  \'' + row[1] + '\', \'' + row[2] + '\',')
                excerpts[1] += ('\n  \'' + row[0] + '\',')
                excerpts[2] += ('\n  \'' + row[0] + '\', \'' + row[1] + '\',')
            for li, lstr in enumerate(excerpts):
                if len(lstr) > 0:
                    excerpts[li]= lstr.rstrip(',') + '\n'
            with open('update_type.txt', 'w') as file:
                file.write("""UPDATE PS_ADM_APPL_DATA
SET ADMIT_TYPE = DECODE(ADM_APPL_NBR, {})
WHERE EMPLID IN ({})
AND ADM_APPL_NBR = DECODE(EMPLID, {});
""".format(*excerpts))
        lcur.execute("""SELECT *
FROM mssbase as msb
INNER JOIN orabase as orb on msb.emplid = orb.emplid and msb.adm_appl_nbr = orb.adm_appl_nbr
WHERE msb.adm_appl_nbr NOT IN (""" + ui + """) AND msb.academic_level != orb.academic_level
ORDER BY 1, 2""")
        ldata = query_to_csv('BAD_LEVEL.csv', lcur, [0, 1, 3])
        if ldata:
            excerpts = ['', '', '']
            for row in ldata:
                excerpts[0] += ('\n  \'' + row[1] + '\', \'' + row[2] + '\',')
                excerpts[1] += ('\n  \'' + row[0] + '\',')
                excerpts[2] += ('\n  \'' + row[0] + '\', \'' + row[1] + '\',')
            for li, lstr in enumerate(excerpts):
                if len(lstr) > 0:
                    excerpts[li]= lstr.rstrip(',') + '\n'
            with open('update_level.txt', 'w') as file:
                file.write("""UPDATE PS_ADM_APPL_DATA
SET ACADEMIC_LEVEL = DECODE(ADM_APPL_NBR, {})
WHERE EMPLID IN ({})
AND ADM_APPL_NBR = DECODE(EMPLID, {});
""".format(*excerpts))
        lcur.execute("""SELECT *
FROM mssbase as msb
INNER JOIN orabase as orb on msb.emplid = orb.emplid and msb.adm_appl_nbr = orb.adm_appl_nbr
WHERE msb.adm_appl_nbr NOT IN (""" + ui + """) AND msb.admit_term != orb.admit_term
ORDER BY 1, 2""")
        ldata = query_to_csv('BAD_TERM.csv', lcur, [0, 1, 4])
        if ldata:
            excerpts = ['', '', '']
            for row in ldata:
                excerpts[0] += ('\n  \'' + row[1] + '\', \'' + row[2] + '\',')
                excerpts[1] += ('\n  \'' + row[0] + '\',')
                excerpts[2] += ('\n  \'' + row[0] + '\', \'' + row[1] + '\',')
            for li, lstr in enumerate(excerpts):
                if len(lstr) > 0:
                    excerpts[li]= lstr.rstrip(',') + '\n'
            with open('update_term.txt', 'w') as file:
                file.write("""UPDATE PS_ADM_APPL_PROG
SET ADMIT_TERM = DECODE(ADM_APPL_NBR, {})
WHERE EMPLID IN ({})
AND ADM_APPL_NBR = DECODE(EMPLID, {});
""".format(*excerpts))
        lcur.execute("""SELECT *
FROM mssbase as msb
INNER JOIN orabase as orb on msb.emplid = orb.emplid and msb.adm_appl_nbr = orb.adm_appl_nbr
WHERE msb.adm_appl_nbr NOT IN (""" + ui + """) AND msb.acad_prog != orb.acad_prog
ORDER BY 1, 2""")
        ldata = query_to_csv('BAD_PROG.csv', lcur, [0, 1, 5])
        if ldata:
            excerpts = ['', '', '']
            for row in ldata:
                excerpts[0] += ('\n  \'' + row[1] + '\', \'' + row[2] + '\',')
                excerpts[1] += ('\n  \'' + row[0] + '\',')
                excerpts[2] += ('\n  \'' + row[0] + '\', \'' + row[1] + '\',')
            for li, lstr in enumerate(excerpts):
                if len(lstr) > 0:
                    excerpts[li]= lstr.rstrip(',') + '\n'
            with open('update_prog.txt', 'w') as file:
                file.write("""UPDATE PS_ADM_APPL_PROG
SET ACAD_PROG = DECODE(ADM_APPL_NBR, {})
WHERE EMPLID IN ({})
AND ADM_APPL_NBR = DECODE(EMPLID, {});
""".format(*excerpts))
        lcur.execute("""SELECT *
FROM mssbase as msb
INNER JOIN orabase as orb on msb.emplid = orb.emplid and msb.adm_appl_nbr = orb.adm_appl_nbr
WHERE msb.adm_appl_nbr NOT IN (""" + ui + """) AND msb.acad_plan != orb.acad_plan
ORDER BY 1, 2""")
        ldata = query_to_csv('BAD_PLAN.csv', lcur, [0, 1, 6])
        if ldata:
            excerpts = ['', '', '']
            for row in ldata:
                excerpts[0] += ('\n  \'' + row[1] + '\', \'' + row[2] + '\',')
                excerpts[1] += ('\n  \'' + row[0] + '\',')
                excerpts[2] += ('\n  \'' + row[0] + '\', \'' + row[1] + '\',')
            for li, lstr in enumerate(excerpts):
                if len(lstr) > 0:
                    excerpts[li]= lstr.rstrip(',') + '\n'
            with open('update_plan.txt', 'w') as file:
                file.write("""UPDATE PS_ADM_APPL_PLAN
SET ACAD_PLAN = DECODE(ADM_APPL_NBR, {})
WHERE EMPLID IN ({})
AND ADM_APPL_NBR = DECODE(EMPLID, {});
""".format(*excerpts))
        lcur.execute("""SELECT *
FROM mssbase as msb
INNER JOIN orabase as orb on msb.emplid = orb.emplid and msb.adm_appl_nbr = orb.adm_appl_nbr
WHERE msb.adm_appl_nbr NOT IN (""" + ui + """) AND msb.prog_action != orb.prog_action
ORDER BY 1, 2""")
        query_to_csv('BAD_ACTION.csv', lcur)
        lcur.execute("""SELECT *
FROM mssbase as msb
INNER JOIN orabase as orb on msb.emplid = orb.emplid and msb.adm_appl_nbr = orb.adm_appl_nbr
WHERE msb.adm_appl_nbr NOT IN (""" + ui + """) AND msb.prog_reason != orb.prog_reason
ORDER BY 1, 2""")
        query_to_csv('BAD_REASON.csv', lcur)
    else:
        print('Missing keys in JSON files; check for proper formation.')
except (OSError, json.JSONDecodeError, cxo.Error, pyodbc.DatabaseError, sqlite3.DatabaseError) as e:
    print(str(e))
finally:
    lconn.rollback()
    lcur.close()
    lconn.close()
