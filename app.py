import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from json.decoder import JSONDecodeError
from twitter_repository import *

import requests

@dataclass(frozen=True)
class Tweet:
    id: str
    username: str
    text: str
    created_at: datetime
    hashtags: []


TWITTER_SAMPLE_STREAM_URL = 'https://api.twitter.com/2/tweets/sample/stream'

delay = timedelta(minutes=1)

twitter_bearer = os.environ['BEARER_TOKEN']


def tabulate(t):
    return f"{t[0] + 1}. \u2067{t[1][0]}\u2069: {len(t[1][1])}\t"


def format_top(tags_stats, top):
    return f'Top {MAX_DISPLAY_HASHTAGS} hashtags: ' + " ".join(
        map(tabulate, enumerate(top))) + f' total {len(tags_stats)}'


def parse_datetime_iso(raw):
    return datetime.fromisoformat(raw.replace('Z', '+00:00'))


def main():
    with get_neo4j_driver() as driver:

        destroy_everything(driver)

        print("Starting Twitter stream ...")

        cnt = 0

        print("Start reading lines")

        params = {'tweet.fields': 'id,text,entities,created_at', 'expansions': 'author_id'}
        headers = {'Authorization': 'Bearer ' + twitter_bearer}

        while True:
            r = requests.get(TWITTER_SAMPLE_STREAM_URL,
                             params=params,
                             headers=headers,
                             stream=True)
            if r.encoding is None:
                r.encoding = 'utf-8'
                print("Set encoding to utf-8")

            for line in r.iter_lines(decode_unicode=True):
                # filter out keep-alive new lines
                if line:
                    try:
                        parsed = json.loads(line)
                        if 'data' in parsed and 'entities' in parsed[
                            'data'] and 'hashtags' in \
                                parsed['data']['entities']:
                            hashtags = list(map(lambda hashtag: hashtag['tag'],
                                                parsed['data']['entities'][
                                                    'hashtags']))

                            print(f"Input parsed {parsed}")

                            tweet = Tweet(
                                id=parsed['data']['id'],
                                username=list(filter(lambda u: u['id'] == parsed['data']['author_id'], parsed['includes']['users']))[0]['username'],
                                text=parsed['data']['text'],
                                created_at=parse_datetime_iso(
                                    parsed['data']['created_at']),
                                hashtags=hashtags
                            )
                            if len(tweet.hashtags) > 0:
                                print(f"Storing tweet #{cnt} {tweet} ...", end="")
                                store_tweet(driver, tweet)
                                delete_old_tweets(driver)
                                cnt += 1
                    except JSONDecodeError as e:
                        print(f"error when parsing json: {e} for line {line}")
    print("Ending")


if __name__ == "__main__":
    main()
