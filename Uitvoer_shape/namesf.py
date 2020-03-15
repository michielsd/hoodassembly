#CREATE TABLE snrefs2019 (code TEXT, sname1 TEXT, surl1 TEXT, sname2 TEXT, surl2 TEXT, sname3 TEXT, surl3 TEXT, sname4 TEXT, surl4 TEXT, sname5 TEXT, surl5 TEXT, sname6 TEXT, surl6 TEXT, sname7 TEXT, surl7 TEXT, sname8 TEXT, surl8 TEXT, sname9 TEXT, surl9 TEXT, sname10 TEXT, surl10 TEXT, sname11 TEXT, surl11 TEXT, sname12 TEXT, surl12 TEXT, sname13 TEXT, surl13 TEXT, sname14 TEXT, surl14 TEXT, sname15 TEXT, surl15 TEXT, nname1 TEXT, nurl1 TEXT, nname2 TEXT, nurl2 TEXT, nname3 TEXT, nurl3 TEXT, nname4 TEXT, nurl4 TEXT, nname5 TEXT, nurl5 TEXT, nname6 TEXT, nurl6 TEXT, nname7 TEXT, nurl7 TEXT, nname8 TEXT, nurl8 TEXT, nname9 TEXT, nurl9 TEXT, nname10 TEXT, nurl10 TEXT, nname11 TEXT, nurl11 TEXT, nname12 TEXT, nurl12 TEXT, nname13 TEXT, nurl13 TEXT, nname14 TEXT, nurl14 TEXT, nname15 TEXT, nurl15 TEXT);

import pandas as pd
from sqlalchemy import create_engine
import psycopg2

try:
    conn = psycopg2.connect("dbname='dbbuurt' user='buurtuser' host='localhost' password='123456'")
    print("Database connection established")
except:
    print("Database connection failed")

cur = conn.cursor()

sname = ['sname%d' % (x) for x in range(1,16)]
surl = ['surl%d' % (x) for x in range(1,16)]
nname = ['nname%d' % (x) for x in range(1,16)]
nurl = ['nurl%d' % (x) for x in range(1,16)]

scols = []
ncols = []
for s in range(0,15):
    scols.extend([sname[s], surl[s]])
    ncols.extend([nname[s], nurl[s]])

cols = ['code'] + scols + ncols
refs = 61 * ['%s']

createphrase = """CREATE TABLE snrefs2019 (%s)""" % (" TEXT, ".join(cols))
insertphrase = """INSERT INTO snrefs2019 (%s) VALUES (%s)""" % (", ".join(cols), ", ".join(refs))

print(insertphrase)
insertphrase = """INSERT INTO snrefs2019 (code, sname1, surl1, sname2, surl2, sname3, surl3, sname4, surl4, sname5, surl5, sname6, surl6, sname7, surl7, sname8, surl8, sname9, surl9, sname10, surl10, sname11, surl11, sname12, surl12, sname13, surl13, sname14, surl14, sname15, surl15, nname1, nurl1, nname2, nurl2, nname3, nurl3, nname4, nurl4, nname5, nurl5, nname6, nurl6, nname7, nurl7, nname8, nurl8, nname9, nurl9, nname10, nurl10, nname11, nurl11, nname12, nurl12, nname13, nurl13, nname14, nurl14, nname15, nurl15) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

#pull data
pullurl = """SELECT codering_3, wijkenenbuurten, urlgm, urlwk, urlbu FROM wijkbuurt2019"""
cur.execute(pullurl)
urldata = cur.fetchall()

codedict = {}
for row in urldata:
    codekey = row[0]
    if codekey.startswith("GM"):
        codevalue = [row[1], row[2]]
        codedict[codekey] = codevalue
    elif codekey.startswith("WK"):
        url = row[2] + "/" + row[3]
        codevalue = [row[1], url]
        codedict[codekey] = codevalue
    elif codekey.startswith("BU"):
        url = row[2] + "/" + row[3] + "/" + row[4]
        codevalue = [row[1], url]
        codedict[codekey] = codevalue

#pull sons, neighbours
pullsons = """SELECT code, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15 FROM sncodes2019"""
cur.execute(pullsons)
sondata = cur.fetchall()

pullneighbours = """SELECT code, n1, n2, n3, n4, n5, n6, n7, n8, n9, n10, n11, n12, n13, n14, n15 FROM sncodes2019"""
cur.execute(pullneighbours)
neighdata = cur.fetchall()

sondict = {}
for row in sondata:
    code = row[0]
    dataline = []
    for item in row[1:16]:
        if item:
            dataline.extend([codedict[item][0], codedict[item][1]])
        else:
            dataline.extend([None, None])

    sondict[code] = dataline

neighdict = {}
for row in neighdata:
    code = row[0]
    dataline = []
    for item in row[1:16]:
        if item:
            dataline.extend([codedict[item][0], codedict[item][1]])
        else:
            dataline.extend([None, None])

    neighdict[code] = dataline    

for row in urldata[1:]:
    code = row[0]
    insertline = tuple([code] + sondict[code] + neighdict[code])
    cur.execute(insertphrase, insertline)
    print("committed %s" % insertline[1])
