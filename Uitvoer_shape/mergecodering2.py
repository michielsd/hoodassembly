import pandas as pd
import shapefile
import shapely.geometry
from shapely.geometry.polygon import Polygon
from simpledbf import Dbf5

#functions

def FindFalsePos(lijst1, lijst2, code):
    

    preselector = []
    for j in lijst1:
        if j not in lijst2 and j.startswith(code):
            preselector.append(j)

    return preselector

def FindCentroid(i, dataobject, code, shapes):


    ranginbestand = dataobject[dataobject[code] == i].index.values.astype(int)[0]
    ishape = shapes.shape(ranginbestand).points
    posn = 0
    loncounter = 0
    latcounter = 0
    for position in ishape:
        posn += 1
        loncounter += position[0]
        latcounter += position[1]
    centroid = shapely.geometry.Point([loncounter / posn, latcounter / posn])

    return centroid

def FindShape(j, dataobject, code, shapes):

    ranginbestand = dataobject[dataobject[code] == j].index.values.astype(int)[0]
    shapeid = shapely.geometry.asShape(shapes.shape(ranginbestand))
    
    return shapeid


#data invoer
gem2018 = pd.read_csv('2019/gm_2019.csv', encoding="Windows-1252")
wijk2018 = pd.read_csv('2019/wk_2019.csv', encoding="Windows-1252")
buurt2018 = pd.read_csv('2019/bu_2019.csv', encoding="Windows-1252")

gem2018s = shapefile.Reader('2019/gm_2019')
wijk2018s = shapefile.Reader('2019/wk_2019')
buurt2018s = shapefile.Reader('2019/bu_2019')

gem2017 = Dbf5('gem_2018.dbf').to_dataframe()
wijk2017 = Dbf5('wijk_2018.dbf').to_dataframe()
buurt2017 = Dbf5('buurt2018.dbf').to_dataframe()

gem2017s = shapefile.Reader('gm2018')
wijk2017s = shapefile.Reader('wk2018')
buurt2017s = shapefile.Reader('bu2018')

data2018 = pd.read_csv('C:/Dashboard/Buurt2/Dataimport/buurtimport/buurtdata2019.csv', encoding="Windows-1252")
data2017 = pd.read_csv('C:/Dashboard/Buurt2/Dataimport/buurtimport/buurtdata2018.csv', encoding="Windows-1252")

data2018['Codering_3'] = data2018['Codering_3'].str.strip()
data2017['Codering_3'] = data2017['Codering_3'].str.strip()

herindeling = {}
herindeling['Groningen'] = [
    2018, {
        'Groningen': 1
        , 'Haren': 1
        , 'Ten Boer': 1
    }
]

herindeling['Het Hogeland'] = [
    2018, {
        'Bedum': 1
        , 'De Marne': 1
        , 'Eemsmond': 1
        , 'Winsum': 0.884
    }
]

herindeling['Westerkwartier'] = [
    2018, {
        'Grootegast': 1
        , 'Leek': 1
        , 'Marum': 1
        , 'Zuidhorn': 1
        , 'Winsum': 0.1157
    }
]

herindeling['Altena'] = [
    2018, {
        'Aalburg': 1
        , 'Werkendam': 1
        , 'Woudrichem': 1
    }
]

herindeling['Beekdaelen'] = [
    2018, {
        'Nuth': 1
        , 'Onderbanken': 1
        , 'Schinnen': 1
    }
]

herindeling['Haarlemmermeer'] = [
    2018, {
        'Haarlemmerliede en Spaarnwoude': 1
        , 'Haarlemmermeer': 1
    }
]

herindeling['Hoeksche Waard'] = [
    2018, {
        'Binnenmaas': 1
        , 'Cromstrijen': 1
        , 'Korendijk': 1
        , 'Oud-Beijerland': 1
        , 'Strijen': 1
    }
]

herindeling['Noardeast-Fryslân'] = [
    2018, {
        'Dongeradeel': 1
        , 'Ferwerderadiel': 1
        , 'Kollumerland en Nieuwkruisland': 1
    }
]

herindeling['Molenlanden'] = [
    2018, {
        'Giessenlanden': 1
        , 'Molenwaard': 1
    }
]

herindeling['Noordwijk'] = [
    2018, {
        'Noordwijk': 1
        , 'Noordwijkerhout': 1
    }
]

herindeling['Vijfheerenlanden'] = [
    2018, {
        'Leerdam': 1
        , 'Zederik': 1
        , 'Vianen': 1
    }
]

herindeling['West Betuwe'] = [
    2018, {
        'Geldermalsen': 1
        , 'Lingewaal': 1
        , 'Neerijnen': 1
    }
]

#in één lijst
lijst2018 = []

for gem in gem2018.iloc[:,0]:
    if gem not in lijst2018:
        lijst2018.append(gem)

for wijk in wijk2018.iloc[:,0]:
    if wijk not in lijst2018:
        lijst2018.append(wijk)

for buurt in buurt2018.iloc[:,0]:
    if buurt not in lijst2018:
        lijst2018.append(buurt)

lijst2017 = []
for gem in gem2017.iloc[:,0]:
    if gem not in lijst2017:
        lijst2017.append(gem)

for wijk in wijk2017.iloc[:,0]:
    if wijk not in lijst2017:
        lijst2017.append(wijk)

for buurt in buurt2017.iloc[:,0]:
    if buurt not in lijst2017:
        lijst2017.append(buurt)

selectgm = FindFalsePos(lijst2017, lijst2018, 'GM')
selectwk = FindFalsePos(lijst2017, lijst2018, 'WK')
selectbu = FindFalsePos(lijst2017, lijst2018, 'BU')

#vind alle indelingswijzigingen

matchdict2017 = {}
for index, row in data2017.iterrows():
    naam = row['WijkenEnBuurten']
    codering = row['Codering_3']
    if codering.startswith("GM"):
        matchdict2017[naam] = codering

matchlist2018 = []
for i in lijst2018:
    try:
        indeling = data2018.loc[
            (data2018['Codering_3'] == i), 'IndelingswijzigingWijkenEnBuurten_4'
        ].values[0]
        naam = data2018.loc[
            (data2018['Codering_3'] == i), 'WijkenEnBuurten'
        ].values[0]
        matchlist2018.append([i, indeling, naam])
    except:
        matchlist2018.append([i])

#dictionary om juiste shapefile te openen (quick and dirty)
sd18 = {
    'GM_CODE': [gem2018, gem2018s]
    , 'WK_CODE': [wijk2018, wijk2018s]
    , 'BU_CODE': [buurt2018, buurt2018s]
}

sd17 = {
    'GM_CODE': [gem2017, gem2017s]
    , 'WK_CODE': [wijk2017, wijk2017s]
    , 'BU_CODE': [buurt2017, buurt2017s]
}

print("data verzameld")

jointlijst = []
gemdict = {}
for i in matchlist2018:
    if len(i) > 1:
        code = i[0]
        indeling = i[1]
        naam = i[2]

        if code.startswith("GM"):
            if naam in herindeling:
                oudecodes = []
                oudecodesnrs = []
                oudegemlijst = herindeling[naam][1]
                for oudegem in oudegemlijst:
                    oudecodes.append(matchdict2017[oudegem])
                    oudecodesnrs.append(matchdict2017[oudegem][2:])

                jointlijst.append([code] + oudecodes)
                print([code] + oudecodes)
                gemdict[code[2:]] = oudecodesnrs
            else:
                jointlijst.append([code, code])
                print([code, code])
                gemdict[code[2:]] = [code[2:]]
        
        else:
            if indeling == '1':
                #zelfde indeling, zelfde code
                jointlijst.append([code, code])
                print([code, code])
            
            elif indeling == '2':
                #zelfde indeling, andere code
                shortcode = code[:6]
                soortcode = "%s_CODE" % (shortcode[:2])
                koppelcodes = gemdict[shortcode[2:]]
                icentroid = FindCentroid(code, sd18[soortcode][0], soortcode, sd18[soortcode][1])

                preselection = []
                for j in range(0, len(koppelcodes)):
                    koppelcode = koppelcodes[j]
                    preselection.append(code[:2] + koppelcode)

                centroidmatches = []
                for k in lijst2017:
                    for l in preselection:
                        if k.startswith(l) and not k.endswith('99'):
                            kshape = FindShape(k, sd17[soortcode][0], soortcode, sd17[soortcode][1])
                            kcentroid = FindCentroid(k, sd17[soortcode][0], soortcode, sd17[soortcode][1])
                            if kshape.contains(icentroid) or ishape.contains(kcentroid):
                                centroidmatches.append(k)

                if len(centroidmatches) == 1:
                    jointlijst.append([code, centroidmatches[0]])
                    print([code, centroidmatches[0]])
                elif len(centroidmatches) > 1:
                    print("centroids:", centroidmatches)
                    distt = []
                    for c in centroidmatches:
                        ccentroid = FindCentroid(c, sd17[soortcode][0], soortcode, sd17[soortcode][1])
                        distt.append([c, icentroid.distance(ccentroid)])

                    distt.sort(key = lambda x: x[1])
                    jointlijst.append([code, c])
                else:
                    print("ERROR IN INDELING 2")
                    print("code", code)
                    print("koppelcodes", koppelcodes)
                    print("preselection", preselection)
                    print(centroidmatches)
                    jointlijst.append([code])

            elif indeling == '3':
                shortcode = code[:6]
                soortcode = "%s_CODE" % (shortcode[:2])
                koppelcodes = gemdict[shortcode[2:]]
                
                ishape = FindShape(code, sd18[soortcode][0], soortcode, sd18[soortcode][1])
                icentroid = FindCentroid(code, sd18[soortcode][0], soortcode, sd18[soortcode][1])

                preselection = []
                for j in range(0, len(koppelcodes)):
                    koppelcode = koppelcodes[j]
                    preselection.append(code[:2] + koppelcode)
                
                matches3 = []
                for k in lijst2017:
                    for l in preselection:
                        if k.startswith(l) and not k.endswith('99'):
                            kshape = FindShape(k, sd17[soortcode][0], soortcode, sd17[soortcode][1])
                            kcentroid = FindCentroid(k, sd17[soortcode][0], soortcode, sd17[soortcode][1])
                            if kshape.contains(icentroid) or ishape.contains(kcentroid):
                                matches3.append(k)

                jointlijst.append([code] + matches3)
                print([code] + matches3)

                if len(matches3) == 0:
                    print("NO MATCHES", code, preselection)
    else:
        print(i)

df = pd.DataFrame.from_records(jointlijst)
df.to_csv("matchlist.csv")