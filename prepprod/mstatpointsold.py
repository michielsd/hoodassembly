import psycopg2
from sqlalchemy import create_engine

import pandas as pd

#GEMIDDELDE INKOMEN ONTBREEKT

try:
    conn = psycopg2.connect("dbname='dbbuurt' user='buurtuser' host='localhost' password='123456'")
    print("Database connection established")
except:
    print("Database connection failed")

cur = conn.cursor()

def FindStats(origindf, targetdf, provs, gems, code):


    def LoopColumn(subframe, totalframe, targetframe, scope, level):
        scope = scope
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
                    targetframe.loc[len(targetframe)] = [scope, column, level, lowest, p10, p25, p50, p75, p90, highest]
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
        da = dfdict[df]
        targetdf = LoopColumn(da, origindf, targetdf, scope, df)
    print('Verwerkt voor land')

    #doe analyse per provincie
    for prov in provset:
        scope = prov

        dg = gmpdict[prov]
        targetdf = LoopColumn(dg, origindf, targetdf, scope, 'GM')

        dw = wkpdict[prov]
        targetdf = LoopColumn(dw, origindf, targetdf, scope, 'WK')

        db = bupdict[prov]
        targetdf = LoopColumn(db, origindf, targetdf, scope, 'BU')
    print('Verwerkt per provincie')
    #doe analyse per gemeente
    for gem in gemeenteset:
        scope = gem

        dw = wkgdict[gem]
        targetdf = LoopColumn(dw, origindf, targetdf, scope, 'WK')

        dg = bugdict[gem]
        targetdf = LoopColumn(dg, origindf, targetdf, scope, 'BU')
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
#stembuurtpull = """SELECT code, vvd, pvda, pvv, sp, cda, d66, christenunie, sgp, pvdd, vijftigplus, denk, fvd FROM stembuurt"""
#df1 = pd.read_sql_query(stembuurtpull, engine)

wijkbuurtpull = """SELECT Codering_3, AantalInwoners_5, Mannen_6, Vrouwen_7, k_0Tot15Jaar_8, k_15Tot25Jaar_9, k_25Tot45Jaar_10, k_45Tot65Jaar_11, k_65JaarOfOuder_12, Ongehuwd_13, Gehuwd_14, Gescheiden_15, Verweduwd_16, WestersTotaal_17, NietWestersTotaal_18, Marokko_19, NederlandseAntillenEnAruba_20, Suriname_21, Turkije_22, OverigNietWesters_23, GeboorteTotaal_24, GeboorteRelatief_25, SterfteTotaal_26, SterfteRelatief_27, HuishoudensTotaal_28, Eenpersoonshuishoudens_29, HuishoudensZonderKinderen_30, HuishoudensMetKinderen_31, GemiddeldeHuishoudensgrootte_32, Bevolkingsdichtheid_33, Woningvoorraad_34, GemiddeldeWoningwaarde_35, PercentageEengezinswoning_36, PercentageMeergezinswoning_37, PercentageBewoond_38, PercentageOnbewoond_39, Koopwoningen_40, HuurwoningenTotaal_41, InBezitWoningcorporatie_42, InBezitOverigeVerhuurders_43, EigendomOnbekend_44, BouwjaarVoor2000_45, BouwjaarVanaf2000_46, GemiddeldElektriciteitsverbruikTotaal_47, Appartement_48, Tussenwoning_49, Hoekwoning_50, TweeOnderEenKapWoning_51, VrijstaandeWoning_52, Huurwoning_53, EigenWoning_54, GemiddeldAardgasverbruikTotaal_55, Appartement_56, Tussenwoning_57, Hoekwoning_58, TweeOnderEenKapWoning_59, VrijstaandeWoning_60, Huurwoning_61, EigenWoning_62, PercentageWoningenMetStadsverwarming_63, AantalInkomensontvangers_64, GemiddeldInkomenPerInkomensontvanger_65, GemiddeldInkomenPerInwoner_66, k_40PersonenMetLaagsteInkomen_67, k_20PersonenMetHoogsteInkomen_68, Actieven1575Jaar_69, k_40HuishoudensMetLaagsteInkomen_70, k_20HuishoudensMetHoogsteInkomen_71, HuishoudensMetEenLaagInkomen_72, HuishOnderOfRondSociaalMinimum_73, PersonenPerSoortUitkeringBijstand_74, PersonenPerSoortUitkeringAO_75, PersonenPerSoortUitkeringWW_76, PersonenPerSoortUitkeringAOW_77, BedrijfsvestigingenTotaal_78, ALandbouwBosbouwEnVisserij_79, BFNijverheidEnEnergie_80, GIHandelEnHoreca_81, HJVervoerInformatieEnCommunicatie_82, KLFinancieleDienstenOnroerendGoed_83, MNZakelijkeDienstverlening_84, RUCultuurRecreatieOverigeDiensten_85, PersonenautoSTotaal_86, PersonenautoSJongerDan6Jaar_87, PersonenautoS6JaarEnOuder_88, PersonenautoSBrandstofBenzine_89, PersonenautoSOverigeBrandstof_90, PersonenautoSPerHuishouden_91, PersonenautoSNaarOppervlakte_92, Motorfietsen_93, AfstandTotHuisartsenpraktijk_94, AfstandTotGroteSupermarkt_95, AfstandTotKinderdagverblijf_96, AfstandTotSchool_97, ScholenBinnen3Km_98, OppervlakteTotaal_99, OppervlakteLand_100, OppervlakteWater_101, MeestVoorkomendePostcode_102, Dekkingspercentage_103, MateVanStedelijkheid_104, Omgevingsadressendichtheid_105, TotaalDiefstalUitWoningSchuurED_106, VernielingMisdrijfTegenOpenbareOrde_107, GeweldsEnSeksueleMisdrijven_108, pctvrouwen_200, pctkinderen_201, pctstudenten_202, pctbijstand_203 FROM wijkbuurt2017"""
df2 = pd.read_sql_query(wijkbuurtpull, engine)

#bereken voor elke categorie 10e, 20e, 50e, 80e en 90e percentiel 
resultdf = pd.DataFrame(
    columns=[
        'scope'
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

#resultdf = FindStats(df1, resultdf, plistdict, glistdict, codestem)
#print("Stemresultaten toegevoegd")
resultdf = FindStats(df2, resultdf, plistdict, glistdict, codebuurt)

#ADD GEMIDDELDE PCT HOOG LAAG INKOMEN + 

#to add gemiddeldinkomen
if resultdf[resultdf['categorie'] == 'gemiddeldinkomenperinkomensontvanger_65'].empty:
    resultdf.loc[len(resultdf)] = ['land', 'gemiddeldinkomenperinkomensontvanger_65', 'GM', '', '', '', 32.0, '', '', '']

if resultdf[resultdf['categorie'] == 'huishoudensmeteenlaaginkomen_72'].empty:
    resultdf.loc[len(resultdf)] == ['land', 'huishoudensmeteenlaaginkomen_72', 'GM', '', '', '', 8.2, '', '', '']

if resultdf[resultdf['categorie'] == 'k_20huishoudensmethoogsteinkomen_71'].empty:
    resultdf.loc[len(resultdf)] == ['land', 'k_20huishoudensmethoogsteinkomen_71', 'GM', '', '', '', 20, '', '', '']

resultdf.to_sql('pctscoresold', engine)
print("Wijken, buurten toegevoegd")
print('Succes!')