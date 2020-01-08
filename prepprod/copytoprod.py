import psycopg2

try:
    conn = psycopg2.connect("dbname='dbbuurt' user='buurtuser' host='localhost' password='123456'")
    print("Database connection established")
except:
    print("Database connection failed")

cur = conn.cursor()

listoftables = [
    'bagadres'
    , 'basisschool'
    , 'bu_post'
    , 'gm_post'
    , 'hboschool'
    , 'kinderopvang'
    , 'mboschool'
    , 'middelschool'
    , 'pctscores'
    , 'stembuurt'
    , 'supermarkt'
    , 'wijkbuurt2018'
    , 'wk_post'
]

dumpphrase = 'pg_dump -U postgres -t %s dbbuurt | psql -U postgres buurtprod'

for table in listoftables:
    print("")
    print(dumpphrase % table)