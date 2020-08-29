import json
import os
from datetime import datetime, timedelta
from json.decoder import JSONDecodeError

import requests
from neo4j import GraphDatabase

MAX_DISPLAY_HASHTAGS = 8

delay = timedelta(minutes=1)

twitter_bearer = os.environ['BEARER_TOKEN']
graphenedb_url = os.environ.get("GRAPHENEDB_BOLT_URL")
graphenedb_user = os.environ.get("GRAPHENEDB_BOLT_USER")
graphenedb_pass = os.environ.get("GRAPHENEDB_BOLT_PASSWORD")

driver = GraphDatabase.driver(graphenedb_url, auth=(graphenedb_user, graphenedb_pass), encrypted=True)

session = driver.session()

session.run("MATCH (:Person {name: 'Tom Hanks'})-[:ACTED_IN]->(tomHanksMovies) RETURN movies")

def print_count(tx):
    for record in tx.run(query):
        print(record["movies"]["title"])

with driver.session() as session:
    session.read_transaction(print_count)

tags_stats = {}

r = requests.get('https://api.twitter.com/2/tweets/sample/stream',
                 params={'tweet.fields': 'entities'},
                 headers={'Authorization': 'Bearer ' + twitter_bearer},
                 stream=True)

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
                    k: updated
                    for
                    k, dates in tags_stats.items() for updated in
                    [list(filter(lambda d: d >= (datetime.now() - delay),
                                 dates))] if len(updated) > 0}
                # print(str(tags_stats))
                top = sorted(tags_stats.items(), key=lambda item: len(item[1]),
                             reverse=True)[:MAX_DISPLAY_HASHTAGS]
                if list(map(lambda a: a[0], top)) != list(
                        map(lambda a: a[0], last)):
                    print(f'Top {MAX_DISPLAY_HASHTAGS} hashtags: ' + format_top(
                        top) + f' total {len(tags_stats)}')
                    last = top
        except JSONDecodeError as e:
            print(f"error when parsing json: {e} for line {line}")
