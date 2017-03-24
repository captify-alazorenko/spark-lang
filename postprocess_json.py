import json

from collections import defaultdict

path = '/home/andriy/Code/IdeaProjects/language-detection/profiles_9/en.json'
common_path = '/home/andriy/Code/IdeaProjects/language-detection/profiles_9/'
langs = ['en', 'de', 'fr']

paths = [common_path + lan + 'json' for lan in langs]

with open(path, 'r') as fh:
    profile = json.load(fh)
# print(profile['a']) # DEBUG

counts = defaultdict(int)
lst = [1, 2, 3, 4, 5, 6, 7, 8, 9]
for key in profile:
    if len(key) in lst:
        counts[len(key)] += profile[key]
counts = dict(counts)
