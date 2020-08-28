import json
import os
from datetime import datetime, timedelta
from json.decoder import JSONDecodeError

import requests

MAX_DISPLAY_HASHTAGS = 8

delay = timedelta(minutes=5)

bearer = os.environ['BEARER_TOKEN']

tags_stats = {}

r = requests.get('https://api.twitter.com/2/tweets/sample/stream',
                 params={'tweet.fields': 'entities'},
                 headers={'Authorization': 'Bearer ' + bearer}, stream=True)

if r.encoding is None:
    r.encoding = 'utf-8'

last = []


def format_top(top):
    return " ".join(
        map(lambda t: f"{t[0] + 1}. \u2067{t[1][0]}\u2069: {len(t[1][1])}\t",
            enumerate(top)))


for line in r.iter_lines(decode_unicode=True):
    # filter out keep-alive new lines
    if line:
        try:
            parsed = json.loads(line)
            if 'data' in parsed and 'entities' in parsed[
                'data'] and 'hashtags' in \
                    parsed['data']['entities']:
                tags = parsed['data']['entities']['hashtags']
                for tag in tags:
                    tag_tag = tag['tag']
                    if tag_tag not in tags_stats:
                        tags_stats[tag_tag] = [datetime.now()]
                    else:
                        tags_stats[tag_tag].append(datetime.now())
                tags_stats = {
                    k: list(filter(lambda d: d >= (datetime.now() - delay),
                                   dates))
                    for
                    k, dates in tags_stats.items()}
                # print(str(tags_stats))
                top = sorted(tags_stats.items(), key=lambda item: len(item[1]),
                             reverse=True)[:MAX_DISPLAY_HASHTAGS]
                if list(map(lambda a: a[0], top)) != list(
                        map(lambda a: a[0], last)):
                    print(f'Top {MAX_DISPLAY_HASHTAGS} hashtags: ' + format_top(
                        top))
                    last = top
        except JSONDecodeError as e:
            print(f"error when parsing json: {e} for line {line}")
