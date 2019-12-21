
teststring = 'Verspreide huizen wijk 21'

if 'wijk' in teststring:
    sp = teststring.find("wijk")
    substring = teststring[sp:(sp+7)]
    if len(substring)>=7 and substring[6].isdigit():
        print("yes")