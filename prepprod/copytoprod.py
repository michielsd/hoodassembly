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

listofwagtails = [
    'auth_group'
    , 'auth_group_permissions' 
    , 'auth_permission'  
    , 'auth_user'
    , 'auth_user_groups' 
    , 'auth_user_user_permissions'
     , 'django_admin_log'
     , 'django_content_type'  
     , 'django_migrations'
     , 'django_session'
     , 'home_homepage' 
     , 'spatial_ref_sys'
     , 'taggit_tag'
     , 'taggit_taggeditem' 
     , 'wagtailcore_collection' 
     , 'wagtailcore_collectionviewrestriction'
     , 'wagtailcore_collectionviewrestriction_groups'
     , 'wagtailcore_groupcollectionpermission'
     , 'wagtailcore_grouppagepermission'
     , 'wagtailcore_page'
     , 'wagtailcore_pagerevision'
     , 'wagtailcore_pageviewrestriction'
     , 'wagtailcore_pageviewrestriction_groups'
     , 'wagtailcore_site'
     , 'wagtaildocs_document'  
     , 'wagtailembeds_embed'
]

dumpphrase = 'pg_dump -U postgres -t %s dbbuurt | psql -U postgres bprod'

dumpphrase2 = 'pg_dump -U postgres -t %s bprod | psql -U postgres buurtprod'

for table in listoftables[:10]:
    print("")
    print(dumpphrase % table)