import pandas as pd

df = pd.read_csv('testmunis.csv')

print(dfr.loc[
    (dfr['Sociaal domein'] > 0)
    & (dfr['Europese Aanbestedingen  Fout'] == None)
    & (dfr['Overige  Onzekerheid'] == None)
])