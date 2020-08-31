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


def tabulate(t):
    return f"{t[0] + 1}. \u2067{t[1][0]}\u2069: {len(t[1][1])}\t"


def format_top(tags_stats, top):
    return f'Top {MAX_DISPLAY_HASHTAGS} hashtags: ' + " ".join(
        map(tabulate, enumerate(top))) + f' total {len(tags_stats)}'


def store_local(tags_stats, tag_tag):
    if tag_tag not in tags_stats:
        tags_stats[tag_tag] = [datetime.now()]
    else:
        tags_stats[tag_tag].append(datetime.now())


def cleanup_local_history(tags_stats):
    return {
        k: updated
        for k, dates in tags_stats.items()
        for updated in [
            list(filter(lambda d: d >= (datetime.now() - delay), dates))
        ]
        if len(updated) > 0
    }


def handle_new_local_top(tags_stats, last):
    top = compute_local_top(tags_stats)
    if list(map(lambda a: a[0], top)) != list(
            map(lambda a: a[0], last)):
        print(format_top(tags_stats, top))
        return top
    return last


def compute_local_top(tags_stats):
    return sorted(
        tags_stats.items(),
        key=lambda item: len(item[1]),
        reverse=True
    )[:MAX_DISPLAY_HASHTAGS]


def parse_datetime_iso(raw):
    return datetime.fromisoformat(raw.replace('Z', '+00:00'))


def merge_tweet(tx, tweet):
    tx.run(
        "MERGE (t: Tweet {id: $id, text: $text, created_at: $created_at})",
        id=tweet.id,
        text=tweet.text,
        created_at=tweet.created_at.isoformat()
    )


def store_tweet(driver, tweet):
    with driver.session() as session:
        session.write_transaction(merge_tweet, tweet)


def main():
    with GraphDatabase.driver(graphenedb_url,
                              auth=(graphenedb_user, graphenedb_pass),
                              encrypted=True) as driver:
        r = requests.get('https://api.twitter.com/2/tweets/sample/stream',
                         params={'tweet.fields': 'id,text,entities,created_at'},
                         headers={'Authorization': 'Bearer ' + twitter_bearer},
                         stream=True)

        if r.encoding is None:
            r.encoding = 'utf-8'

        last = []

        tags_stats = {}

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
                            created_at=parse_datetime_iso(
                                parsed['data']['created_at']),
                            hashtags=map(lambda hashtag: hashtag['tag'],
                                         parsed['data']['entities']['hashtags'])
                        )
                        for tag in tweet.hashtags:
                            store_local(tags_stats, tag)
                        tags_stats = cleanup_local_history(tags_stats)
                        last = handle_new_local_top(tags_stats, last)
                        store_tweet(driver, tweet)
                except JSONDecodeError as e:
                    print(f"error when parsing json: {e} for line {line}")


if __name__ == "__main__":
    main()
