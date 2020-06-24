import re
Word_re=re.compile(r'[\w]+')
word=Word_re.findall('Big data, hadoop and map reduce. (Nice thing to explore.)')
print(word)