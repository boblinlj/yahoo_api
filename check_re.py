import re

txt = 'AAPL231229C00050000'

pattern = re.compile(r'[A-Za-z]+|\d+')

print(re.findall(pattern, txt))