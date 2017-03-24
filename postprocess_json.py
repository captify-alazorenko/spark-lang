import json

path = '/home/andriy/Code/IdeaProjects/language-detection/profiles_9/en.json'
common_path = '/home/andriy/Code/IdeaProjects/language-detection/profiles_9/'
langs = ['en', 'de', 'fr']

paths = [common_path + lan + 'json' for lan in langs]

with open(path, 'r') as fh:
    profile = json.load(fh)
print(profile['a'])

counts = dict()
for key in profile:
    if len(key) == 1:


