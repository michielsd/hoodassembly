import cbsodata
import csv

"""
datasets

84286NED: wijken buurten 2018
84415NED: gemeentelijke heffingen
83765NED: wijken buurten 2017
83487NED: wijken buurten 2016
83220NED: wijken buurten 2015
82931NED: wijken buurten 2014
82339NED: wijken buurten 2013


"""

info = cbsodata.get_data('82339NED')

infolist = []

#create list of keys on row 1
rowlist = []
for key, value in info[0].items():
    rowlist.append(key)
infolist.append(rowlist)


#create list of values per gemeente/wijk/buurt
for row in info:
    rowlist = []
    for key, value in info[info.index(row)].items():
        rowlist.append(value)
    infolist.append(rowlist)


#write to file
filename = 'buurtdata2013.csv'
with open(filename, 'w', newline='') as newfile:
        writer = csv.writer(newfile)
        writer.writerows(infolist)


"""
#create postgres inter
#create list of keys on row 1

keylist = []
for key, value in info[0].items():
    keylist.append(key)


#create list of values per gemeente/wijk/buurt
valuelist = [] 
for key, value in info[860].items():
    valuelist.append(value)

for keyrow in keylist:
    valuerow = valuelist[keylist.index(keyrow)]
    keytype = ""
    if isinstance(valuerow, str):
        keytype = "TEXT,"
    elif isinstance(valuerow, int):
        keytype = "INT,"
    elif isinstance(valuerow, float):
        keytype = "FLOAT(8),"
    print(keyrow, keytype)

"""