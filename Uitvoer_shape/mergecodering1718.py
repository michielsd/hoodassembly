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
gem2018 = Dbf5('gem_2018.dbf').to_dataframe()
wijk2018 = Dbf5('wijk_2018.dbf').to_dataframe()
buurt2018 = Dbf5('buurt2018.dbf').to_dataframe()

gem2018s = shapefile.Reader('gem_2018')
wijk2018s = shapefile.Reader('wijk_2018')
buurt2018s = shapefile.Reader('buurt2018')

gem2017 = Dbf5('gem_2017.dbf').to_dataframe()
wijk2017 = Dbf5('wijk_2017.dbf').to_dataframe()
buurt2017 = Dbf5('buurt_2017.dbf').to_dataframe()

gem2017s = shapefile.Reader('gem_2017')
wijk2017s = shapefile.Reader('wijk_2017')
buurt2017s = shapefile.Reader('buurt_2017')

data2018 = pd.read_csv('C:/Dashboard/Buurt2/Dataimport/buurtimport/buurtdata2018.csv', encoding="Windows-1252")
data2017 = pd.read_csv('C:/Dashboard/Buurt2/Dataimport/buurtimport/buurtdata2017.csv', encoding="Windows-1252")

data2018['Codering_3'] = data2018['Codering_3'].str.strip()
data2017['Codering_3'] = data2017['Codering_3'].str.strip()

herindeling = {}
herindeling['Leeuwarden'] = [
    2017, {
        'Leeuwarden': 1
        , 'Leeuwarderadeel': 1
        , 'Littenseradiel': 0.32
    }
]
herindeling['Midden-Groningen'] = [
    2017, {
        'Hoogezand-Sappemeer': 1
        , 'Menterwolde': 1
        , 'Slochteren': 1
    }
]
herindeling['Waadhoeke'] = [
    2017, {
        'Franekeradeel': 1
        , 'het Bildt': 1
        , 'Menameradiel': 1
        , 'Littenseradiel': 0.17
    }
]
herindeling['Westerwolde'] = [
    2017, {
        'Bellingwedde': 1
        , 'Vlagtwedde': 1
    }
]
herindeling['Zevenaar'] = [
    2017, {
        'Rijnwaarden': 1
        , 'Zevenaar': 1
    }
]

"""
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
"""

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
                centroid = FindCentroid(code, sd18[soortcode][0], soortcode, sd18[soortcode][1])

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
                        if k.startswith(l):
                            kshape = FindShape(k, sd17[soortcode][0], soortcode, sd17[soortcode][1])
                            kcentroid = FindCentroid(k, sd17[soortcode][0], soortcode, sd17[soortcode][1])
                            if kshape.contains(icentroid) or ishape.contains(kcentroid):
                                matches3.append(k)

                jointlijst.append([code] + matches3)
                print([code] + matches3)

                if len(matches3) == 0:
                    print("NO MATCHES", code, preselection)

df = pd.DataFrame.from_records(jointlijst)
df.to_csv("matchlist.csv")



"""
else:
        if i.startswith("GM"):
            gmmatches = []
            ishape = FindShape(i, gem2018, 'GM_CODE', gem2018s)

            for gm in lijst2017:
                if gm.startswith(i[:3]):
                    gmshape = FindShape(gm, gem2017, 'GM_CODE', gem2017s)
                    if gmshape.intersects(ishape):
                        gmmatches.append(gm)

            #for gm in selectgm:
            #    gmshape = FindShape(gm, gem2017, 'GM_CODE', gem2017s)
            #    if gmshape.intersects(ishape):
            #        gmmatches.append(gm)

            jointlijst.append([i] + gmmatches)
            gmdict[i] = gmmatches
            print([i] + gmmatches)

        elif i.startswith("WK"):
            wkmatches = []
            ishape = FindShape(i, wijk2018, 'WK_CODE', wijk2018s)

            # match voor wijken in de buurt
            for wk in lijst2017:
                if wk.startswith(i[:5]):
                    wkshape = FindShape(wk, wijk2017, 'WK_CODE', wijk2017s)
                    if wkshape.intersects(ishape):
                        wkmatches.append(wk)

            # match voor heringedeelde gemeenten
            if not wkmatches:
                gmkey = "GM%s" % (i[2:-2])
                gmlist = gmdict[gmkey]
                print(gmkey, gmlist)

                for wk in lijst2017:
                    for gwk in gmlist:
                        if wk.startswith("WK%s" % (gwk[2:])):
                            print(wk)
                            wkshape = FindShape(wk, wijk2017, 'WK_CODE', wijk2017s)
                            if wkshape.intersects(ishape):
                                wkmatches.append(wk)

            # vangt de rest
            if not wkmatches:
                for wk in lijst2017:
                    if wk.startswith("WK"):
                        wkshape = FindShape(wk, wijk2017, 'WK_CODE', wijk2017s)
                        if wkshape.intersects(ishape):
                            wkmatches.append(wk)

            jointlijst.append([i] + wkmatches)
            wkdict[i] = wkmatches
            print([i] + wkmatches)

        elif i.startswith("BU"):
            bumatches = []
            ishape = FindShape(i, buurt2018, 'BU_CODE', buurt2018s)

            # match voor buurten in de buurt
            for bu in lijst2017:
                if bu.startswith(i[:5]):
                    bushape = FindShape(bu, buurt2017, 'BU_CODE', buurt2017s)
                    if bushape.intersects(ishape):
                        bumatches.append(bu)

            # match voor buurten in heringedeelde wijken
            if not wkmatches:
                wkkey = "WK%s" % (i[2:-3])
                wklist = wkdict[wkkey]
                print(wkkey, wklist)
                for wk in lijst2017:
                    for gbu in wklist:
                        if wk.startswith("BU%s" % (gbu[2:])):
                            wkshape = FindShape(wk, buurt2017, 'BU_CODE', buurt2017s)
                            if wkshape.intersects(ishape):
                                bumatches.append(wk)            

            # voor de rest
            if not bumatches:
                for wk in lijst2017:
                    if wk.startswith("BU"):
                        wkshape = FindShape(wk, buurt2017, 'BU_CODE', buurt2017s)
                        if wkshape.intersects(ishape):
                            wkmatches.append(wk)

            jointlijst.append([i] + bumatches)
            print([i] + bumatches)




"""