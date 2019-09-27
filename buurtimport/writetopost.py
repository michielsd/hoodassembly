import csv
import psycopg2

try:
    conn = psycopg2.connect("dbname='dbbuurt' user='buurtuser' host='localhost' password='123456'")
    print("Database connection established")
except:
    print("Database connection failed")

cur = conn.cursor()

with open('buurtdata2013.csv', newline='') as csvfile:
    buurttable = list(list(rec) for rec in csv.reader(csvfile, delimiter=',', quotechar='"'))

for row in buurttable:
    for column in range(0, len(row)):
        cell = row[column]
        if cell == '':
            row[column] = None
        elif "." in cell:
            for char in cell:
                if char.isdigit():
                    pass
                else:
                    row[column] = None
        elif isinstance(cell, str):
            row[column] = cell.strip(' ')

"""
gemeentetable = []
for row in buurttable:
    if row == buurttable[0]:
        gemeentetable.append(row)
    elif row[3] == 'Gemeente':
        gemeentetable.append(row)

filename = 'gemeentelijst.csv'

with open(filename, 'w', newline='') as newfile:
        writer = csv.writer(newfile)
        writer.writerows(gemeentetable)
"""

insertphrase = """INSERT INTO wijkbuurt2013 (ID, RegioS, Gemeentenaam_1, SoortRegio_2, Codering_3, AantalInwoners_4, Mannen_5, Vrouwen_6, k_0Tot15Jaar_7, k_15Tot25Jaar_8, k_25Tot45Jaar_9, k_45Tot65Jaar_10, k_65JaarOfOuder_11, Ongehuwd_12, Gehuwd_13, Gescheiden_14, Verweduwd_15, WestersTotaal_16, NietWestersTotaal_17, Marokko_18, NederlandseAntillenEnAruba_19, Suriname_20, Turkije_21, OverigNietWesters_22, GeboorteTotaal_23, GeboorteRelatief_24, SterfteTotaal_25, SterfteRelatief_26, HuishoudensTotaal_27, Eenpersoonshuishoudens_28, HuishoudensZonderKinderen_29, HuishoudensMetKinderen_30, GemiddeldeHuishoudensgrootte_31, Bevolkingsdichtheid_32, Woningvoorraad_33, GemiddeldeWoningwaarde_34, PercentageEengezinswoning_35, PercentageMeergezinswoning_36, PercentageBewoond_37, PercentageLeegstaand_38, Koopwoningen_39, HuurwoningenTotaal_40, InBezitWoningcorporatie_41, InBezitOverigeVerhuurders_42, EigendomOnbekend_43, BouwjaarVoor2000_44, BouwjaarVanaf2000_45, GemiddeldElektriciteitsverbruikTotaal_46, Appartement_47, Tussenwoning_48, Hoekwoning_49, TweeOnderEenKapWoning_50, VrijstaandeWoning_51, Huurwoning_52, Koopwoning_53, GemiddeldAardgasverbruikTotaal_54, Appartement_55, Tussenwoning_56, Hoekwoning_57, TweeOnderEenKapWoning_58, VrijstaandeWoning_59, Huurwoning_60, Koopwoning_61, PercentageWoningenMetStadsverwarming_62, AantalInkomensontvangers_63, GemiddeldInkomenPerInkomensontvanger_64, GemiddeldInkomenPerInwoner_65, PersonenMetLaagInkomen_66, PersonenMetHoogInkomen_67, NietActieven_68, HuishoudensMetLaagInkomen_69, HuishoudensMetHoogInkomen_70, HuishoudensMetLageKoopkracht_71, HuishOnderOfRondSociaalMinimum_72, PersonenMetEenWWBUitkeringTotaal_73, PersonenMetEenAOUitkeringTotaal_74, PersonenMetEenWWUitkeringTotaal_75, PersonenMetEenAOWUitkeringTotaal_76, BedrijfsvestigingenTotaal_77, ALandbouwBosbouwEnVisserij_78, BFNijverheidEnEnergie_79, GIHandelEnHoreca_80, HJVervoerInformatieEnCommunicatie_81, KLFinancieleDienstenOnroerendGoed_82, MNZakelijkeDienstverlening_83, RUCultuurRecreatieOverigeDiensten_84, PersonenautoSTotaal_85, PersonenautoSJongerDan6Jaar_86, PersonenautoS6JaarEnOuder_87, PersonenautoSBrandstofBenzine_88, PersonenautoSOverigeBrandstof_89, PersonenautoSPerHuishouden_90, PersonenautoSNaarOppervlakte_91, Bedrijfsmotorvoertuigen_92, Motortweewielers_93, AfstandTotHuisartsenpraktijk_94, AfstandTotGroteSupermarkt_95, AfstandTotKinderdagverblijf_96, AfstandTotSchool_97, ScholenBinnen3Km_98, OppervlakteTotaal_99, OppervlakteLand_100, OppervlakteWater_101, StedelijkBodemgebruikTotaal_102, Verkeersterrein_103, BebouwdTerrein_104, SemiBebouwdTerrein_105, Recreatieterrein_106, StedelijkBodemgebruikTotaal_107, Verkeersterrein_108, BebouwdTerrein_109, SemiBebouwdTerrein_110, Recreatieterrein_111, NietStedelijkBodemgebruikTotaal_112, AgrarischTerrein_113, BosEnOpenNatuurlijkTerrein_114, NietStedelijkBodemgebruikTotaal_115, AgrarischTerrein_116, BosEnOpenNatuurlijkTerrein_117, MeestVoorkomendePostcode_118, Dekkingspercentage_119, MateVanStedelijkheid_120, Omgevingsadressendichtheid_121, IndelingswijzigingWijkenEnBuurten_122) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

for row in buurttable[1:]:
    cur.execute(insertphrase, row)
    print(row[1])

conn.commit()
conn.close()

print("")
print('success!')
