import csv
import re

filename = 'buurtdata2013.csv'
dbname = 'dbbuurt'
tablename = 'wijkbuurt2013'

with open(filename, newline='') as csvfile:
    csvtable = list(list(rec) for rec in csv.reader(csvfile, delimiter=',', quotechar='"'))

#create postgres inter
#create list of keys on row 1

variablelist = []
for variable in csvtable[0]:
    variablelist.append(variable)

#create list of values per gemeente/wijk/buurt
valuelist = [] 
for value in csvtable[1]:
    valuelist.append(value)

createdb = []
putindb1 = []
putindb2 = []
for keyrow in variablelist:
    valuerow = valuelist[variablelist.index(keyrow)]
    keytype = ""
    if "." in valuerow:
        keytype = "FLOAT(8)"
    elif re.search('[a-zA-Z]', valuerow):
        keytype = "TEXT"
    else:
        keytype = "INT"
    createdb.append(keyrow + ' ' + keytype)
    putindb1.append(keyrow)
    putindb2.append("%s")

createstr = "CREATE TABLE " + tablename + " (" + ', '.join(createdb) + ");"
putinstr = "INSERT INTO " + dbname + "." +   tablename + " (" + ', '.join(putindb1) + ") VALUES (" + ', '.join(putindb2) + ")"

print("")
print(createstr)
print("")
print("")
print(putinstr)
print("")
