import os
import requests
import json

bearer = os.environ['BEARER_TOKEN']

r = requests.get('https://api.twitter.com/2/tweets/sample/stream', params={'tweet.fields':'entities'}, headers={'Authorization': 'Bearer ' + bearer}, stream=True)

if r.encoding is None:
    r.encoding = 'utf-8'

for line in r.iter_lines(decode_unicode=True):

    # filter out keep-alive new lines
    if line:
        parsed = json.loads(line)
        if 'data' in parsed and 'entities' in parsed['data'] and 'hashtags' in parsed['data']['entities']:
            print(parsed['data']['entities']['hashtags'])
