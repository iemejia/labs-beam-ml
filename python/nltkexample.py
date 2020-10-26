import json

# nltk requires a weird extra setup step either:
# nltk.download() or python -m nltk.downloader all

from nltk.tokenize import sent_tokenize, word_tokenize

element = '{ "book":"Test Book", "sentence":"All work and no play makes jack a dull boy, all work and no play. All work and no play makes jack a dull boy, all work and no play. All work and no play makes jack a dull boy, all work and no play."}'
input = json.loads(element)

tokenized_text = sent_tokenize(input['sentence'])
print(tokenized_text)
