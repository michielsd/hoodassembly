import re

searchinput = "Johannes vander Waalsstraat72"

searchnumber = re.search(r'\d+', searchinput).group(0)
searchtext = re.findall(r'\D+', searchinput)[0].split()

print(searchtext)

results = []
if searchnumber:
    results.append(searchnumber)
if searchtext:
    results.extend(searchtext)

print(results)

tester = []

if len(tester) == 0:
    print("WORKS")

for r in tester[1:]:
    print(r)