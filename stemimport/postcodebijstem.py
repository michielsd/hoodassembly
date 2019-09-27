import mysql.connector

con = mysql.connector.Connect(user='root', password='Dem3006', database='verkiezingsuitslag', host='localhost', charset = 'utf8')
cur = con.cursor()

# formule om toe te voegen aan MYSQL
def insertpostcode(idnr, post):
	add_data = "UPDATE stembureau SET postcode = '%s' WHERE id = '%s'" % (post, idnr)
	return add_data

#import stembureaus from dbase
stembureaucaller = ("SELECT * FROM stembureau")
cur.execute(stembureaucaller)
stembureautuple = cur.fetchall()

stembureautabel = []
for row in stembureautuple:
	stembureautabel.append(list(row))

#import stembureaus from dbase and create ref vector
postbureaucaller = ("SELECT * FROM verkiezingsuitslag.postbureau")
cur.execute(postbureaucaller)
postbureautuple = cur.fetchall()

postref = []
postcodetabel = []
for row in postbureautuple:
	postref.append(row[2])
	postcodetabel.append(row[-1])

#match stembureau w/ postcode
for row in stembureautabel:
	if row[1] in postref:
		row.append(postcodetabel[postref.index(row[1])])
		if row[-1] != 'ERROR':
			cur.execute(insertpostcode(row[0],row[-1]))

con.commit()
cur.close()
con.close()

print('success!')