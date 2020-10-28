input = '{ "book":"Test Book", "sentence":"All work and no play makes jack a dull boy, all work and no play. All work and no play makes jack a dull boy, all work and no play. All work and no play makes jack a dull boy, all work and no play."}'

import json
element = json.loads(input)

# nltk requires a weird extra setup step either:
# nltk.download() or python -m nltk.downloader all
from nltk.tokenize import sent_tokenize
phrases = sent_tokenize(element['sentence'])
output = [element['book'] + ': ' + x for x in phrases]

# output = 'single'
# if not isinstance(output, list):
#     output = [output]
print(output)
