import psycopg2
from sqlalchemy import create_engine

import pandas as pd

try:
    conn = psycopg2.connect("dbname='dbbuurt' user='buurtuser' host='localhost' password='123456'")
    print("Database connection established")
except:
    print("Database connection failed")

cur = conn.cursor()

def FindStats(origindf, targetdf, provs, gems, code):


    def LoopColumn(subframe, totalframe, targetframe, scope, scopecat, level):
        scope = scope
        scopecat = scopecat
        for column in subframe.columns[1:]:
            if not subframe[column].isnull().all():
                try:
                    lowest = totalframe.iloc[subframe[column].idxmin()][code]
                    p10 = subframe[column].quantile(0.1)
                    p25 = subframe[column].quantile(0.25)
                    p50 = subframe[column].quantile(0.5)
                    p75 = subframe[column].quantile(0.75)
                    p90 = subframe[column].quantile(0.9)
                    highest = totalframe.iloc[subframe[column].idxmax()][code]
                    targetframe.loc[len(targetframe)] = [scope, scopecat, column, level, lowest, p10, p25, p50, p75, p90, highest]
                except:
                    pass

        return targetframe


    #verdeel dataframe in gemeenten, wijken en buurten
    dfdict = {}
    dfdict['GM'] = origindf.loc[origindf[code].str.startswith('GM')]
    dfdict['WK'] = origindf.loc[origindf[code].str.startswith('WK')]
    dfdict['BU'] = origindf.loc[origindf[code].str.startswith('BU')]

    #verdeel frames wijken en buurten per provincie en gemeente
    gmpdict = {}
    wkpdict = {}
    bupdict = {}
    wkgdict = {}
    bugdict = {}
    dg = dfdict['GM']
    dw = dfdict['WK']
    db  = dfdict['BU']

    for prov in provs:
        plijst = provs[prov]
        gframe = dg[dg[code].isin(plijst)]
        wframe = dw[dw[code].isin(plijst)]
        bframe = db[db[code].isin(plijst)]
        gmpdict[prov] = gframe
        wkpdict[prov] = wframe
        bupdict[prov] = bframe

    for gem in gems:
        glijst = gems[gem]
        wframe = dw[dw[code].isin(glijst)]
        bframe = db[db[code].isin(glijst)]
        wkgdict[gem] = wframe
        bugdict[gem] = bframe

    #doe analyse voor hele land
    for df in dfdict:
        scope = 'land'
        scopecat = 'land'
        da = dfdict[df]
        targetdf = LoopColumn(da, origindf, targetdf, scope, scopecat, df)
    print('Verwerkt voor land')

    #doe analyse per provincie
    for prov in provset:
        scope = prov
        scopecat = 'provincie'
        dg = gmpdict[prov]
        targetdf = LoopColumn(dg, origindf, targetdf, scope, scopecat, 'GM')

        dw = wkpdict[prov]
        targetdf = LoopColumn(dw, origindf, targetdf, scope, scopecat, 'WK')

        db = bupdict[prov]
        targetdf = LoopColumn(db, origindf, targetdf, scope, scopecat, 'BU')
    print('Verwerkt per provincie')
    #doe analyse per gemeente
    for gem in gemeenteset:
        scope = gem
        scopecat = 'gemeente'

        dw = wkgdict[gem]
        targetdf = LoopColumn(dw, origindf, targetdf, scope, scopecat, 'WK')

        dg = bugdict[gem]
        targetdf = LoopColumn(dg, origindf, targetdf, scope, scopecat, 'BU')
    print('Verwerkt per gemeente')
    return targetdf

# bereken: nationaal, per stad - zowel voor buurt als voor wijk als voor gemeente
# bereken: 10e percentiel, 20e percentiel, gemiddelde, 80e percentiel, 90e percentiel
#

engine = create_engine('postgresql+psycopg2://buurtuser:123456@localhost/dbbuurt')

#haal provincies en gemeenten binnen
provpull = """SELECT code, gemeente, provincie FROM provbase"""
cur.execute(provpull)
provtuple = cur.fetchall()

gemeentedict = {}
provdict = {}
gemeenteset = []
provset = []
for row in provtuple:
    gemeentedict[row[0]] = row[1]
    provdict[row[0]] = row[2]
    gemeenteset.append(row[1])
    provset.append(row[2])

gemeenteset = set(gemeenteset)
provset = set(provset)

glistdict = {}
for gemeente in gemeenteset:
    gemeentelijst = []
    for entry in gemeentedict:
        if gemeentedict[entry] == gemeente:
            gemeentelijst.append(entry)
    glistdict[gemeente] = gemeentelijst

plistdict = {}
for provincie in provset:
    provlijst = []
    for entry in provdict:
        if provdict[entry] == provincie:
            provlijst.append(entry)
    plistdict[provincie] = provlijst

# basisschool, hboschool, kinderopvang, mboschool, middelschool
# meeste binnen gemeente / provincie / stad: moet nieuw tabel voor komen

#############
# stembuurt #
#############
stembuurtpull = """SELECT code, vvd, pvda, pvv, sp, cda, d66, christenunie, groenlinks, sgp, pvdd, vijftigplus, denk, fvd FROM stembuurt"""
df1 = pd.read_sql_query(stembuurtpull, engine)

stemmenpull = """SELECT code, popupct FROM stemmen"""
df2 = pd.read_sql_query(stemmenpull, engine)

wijkbuurtpull = """SELECT codering_3, aantalinwoners_5, mannen_6, vrouwen_7, k_0tot15jaar_8, k_15tot25jaar_9, k_25tot45jaar_10, k_45tot65jaar_11, k_65jaarofouder_12, ongehuwd_13, gehuwd_14, gescheiden_15, verweduwd_16, westerstotaal_17, nietwesterstotaal_18, marokko_19, nederlandseantillenenaruba_20, suriname_21, turkije_22, overignietwesters_23, geboortetotaal_24,  geboorterelatief_25, sterftetotaal_26, sterfterelatief_27, huishoudenstotaal_28, eenpersoonshuishoudens_29, huishoudenszonderkinderen_30, huishoudensmetkinderen_31, gemiddeldehuishoudensgrootte_32, bevolkingsdichtheid_33, woningvoorraad_34, gemiddeldewoningwaarde_35, percentageeengezinswoning_36, percentagemeergezinswoning_37, percentagebewoond_38, percentageonbewoond_39, koopwoningen_40, huurwoningentotaal_41,  inbezitwoningcorporatie_42, inbezitoverigeverhuurders_43, eigendomonbekend_44,  bouwjaarvoor2000_45,  bouwjaarvanaf2000_46,  gemiddeldelektriciteitsverbruiktotaal_47,  appartement_48, tussenwoning_49, hoekwoning_50,  tweeondereenkapwoning_51, vrijstaandewoning_52, huurwoning_53, eigenwoning_54, gemiddeldaardgasverbruiktotaal_55, appartement_56, tussenwoning_57, hoekwoning_58, tweeondereenkapwoning_59, vrijstaandewoning_60,  huurwoning_61, eigenwoning_62, percentagewoningenmetstadsverwarming_63,  aantalinkomensontvangers_64, gemiddeldinkomenperinkomensontvanger_65,  gemiddeldinkomenperinwoner_66, k_40personenmetlaagsteinkomen_67,  k_20personenmethoogsteinkomen_68,  actieven1575jaar_69,  k_40huishoudensmetlaagsteinkomen_70, k_20huishoudensmethoogsteinkomen_71, huishoudensmeteenlaaginkomen_72, huishonderofrondsociaalminimum_73,  personenpersoortuitkeringbijstand_74, personenpersoortuitkeringao_75,  personenpersoortuitkeringww_76, personenpersoortuitkeringaow_77,  totaaldiefstaluitwoningschuured_78, vernielingmisdrijftegenopenbareorde_79, geweldsenseksuelemisdrijven_80, bedrijfsvestigingentotaal_81, alandbouwbosbouwenvisserij_82,  bfnijverheidenenergie_83, gihandelenhoreca_84, hjvervoerinformatieencommunicatie_85, klfinancieledienstenonroerendgoed_86, mnzakelijkedienstverlening_87,  rucultuurrecreatieoverigediensten_88, personenautostotaal_89,  personenautosbrandstofbenzine_90, personenautosoverigebrandstof_91, personenautosperhuishouden_92, personenautosnaaroppervlakte_93, motorfietsen_94, afstandtothuisartsenpraktijk_95, afstandtotgrotesupermarkt_96, afstandtotkinderdagverblijf_97, afstandtotschool_98, scholenbinnen3km_99, oppervlaktetotaal_100, oppervlakteland_101, oppervlaktewater_102, meestvoorkomendepostcode_103, dekkingspercentage_104, matevanstedelijkheid_105, omgevingsadressendichtheid_106, pctvrouwen_200, pctkinderen_201, pctstudenten_202, pctmarok_203, pctturk_204, pctantilsur_205, pctoverigallo_206, pctdiefstal_207, pctvernieling_208, pctgeweld_209, gemiddeldinkomen_210, hhhooginkomen_211, hhlaaginomen_212, pctbijstand_213 FROM wijkbuurt2018"""
df3 = pd.read_sql_query(wijkbuurtpull, engine)

#bereken voor elke categorie 10e, 20e, 50e, 80e en 90e percentiel 
resultdf = pd.DataFrame(
    columns=[
        'scope'
        , 'scopecat'
        , 'categorie'
        , 'level'
        , 'lowest'
        , 'p10'
        , 'p25'
        , 'p50'
        , 'p75'
        , 'p90'
        , 'highest'
        ]
)

codestem = 'code'
codebuurt = 'codering_3'

resultdf = FindStats(df1, resultdf, plistdict, glistdict, codestem)
resultdf = FindStats(df2, resultdf, plistdict, glistdict, codestem)
print("Stemresultaten toegevoegd")

resultdf = FindStats(df3, resultdf, plistdict, glistdict, codebuurt)

resultdf.to_sql('pctscores', engine)
print("Wijken, buurten toegevoegd")
print('Succes!')