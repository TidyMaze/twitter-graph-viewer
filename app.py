import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from json.decoder import JSONDecodeError

import requests
from neo4j import GraphDatabase


@dataclass(frozen=True)
class Tweet:
    id: str
    text: str
    created_at: datetime
    hashtags: []


MAX_DISPLAY_HASHTAGS = 8

delay = timedelta(minutes=1)

twitter_bearer = os.environ['BEARER_TOKEN']
graphenedb_url = os.environ.get("GRAPHENEDB_BOLT_URL")
graphenedb_user = os.environ.get("GRAPHENEDB_BOLT_USER")
graphenedb_pass = os.environ.get("GRAPHENEDB_BOLT_PASSWORD")

driver = GraphDatabase.driver(graphenedb_url,
                              auth=(graphenedb_user, graphenedb_pass),
                              encrypted=True)

session = driver.session()

for record in session.run("MATCH (n:Movie) RETURN n LIMIT 25"):
    print(record)

session.close()
driver.close()

tags_stats = {}

r = requests.get('https://api.twitter.com/2/tweets/sample/stream',
                 params={'tweet.fields': 'entities'},
                 headers={'Authorization': 'Bearer ' + twitter_bearer},
                 stream=True)

if r.encoding is None:
    r.encoding = 'utf-8'

last = []


def tabulate(t):
    return f"{t[0] + 1}. \u2067{t[1][0]}\u2069: {len(t[1][1])}\t"


def format_top(top):
    return f'Top {MAX_DISPLAY_HASHTAGS} hashtags: ' + " ".join(
        map(tabulate, enumerate(top))) + f' total {len(tags_stats)}'


def store_local(tag_tag):
    if tag_tag not in tags_stats:
        tags_stats[tag_tag] = [datetime.now()]
    else:
        tags_stats[tag_tag].append(datetime.now())


def cleanup_local_history():
    global tags_stats
    tags_stats = {
        k: updated
        for
        k, dates in tags_stats.items() for updated in
        [list(filter(lambda d: d >= (datetime.now() - delay),
                     dates))] if len(updated) > 0}


def handle_new_local_top():
    global last
    top = compute_local_top()
    if list(map(lambda a: a[0], top)) != list(
            map(lambda a: a[0], last)):
        print(format_top(
            top))
        last = top


def compute_local_top():
    return sorted(tags_stats.items(), key=lambda item: len(item[1]),
                  reverse=True)[:MAX_DISPLAY_HASHTAGS]


def parse_datetime_iso(raw):
    return datetime.fromisoformat(raw.replace('Z', '+00:00'))


for line in r.iter_lines(decode_unicode=True):
    # filter out keep-alive new lines
    if line:
        try:
            parsed = json.loads(line)
            if 'data' in parsed and 'entities' in parsed[
                'data'] and 'hashtags' in \
                    parsed['data']['entities']:
                tweet = Tweet(
                    id=parsed['data']['id'],
                    text=parsed['data']['text'],
                    created_at=parse_datetime_iso(parsed['data']['created_at']),
                    hashtags=map(lambda hashtag: hashtag['tag'],
                                 parsed['data']['entities']['hashtags'])
                )
                for tag in tweet.hashtags:
                    store_local(tag)
                cleanup_local_history()
                handle_new_local_top()
        except JSONDecodeError as e:
            print(f"error when parsing json: {e} for line {line}")
