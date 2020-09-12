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


MAX_NODES_ALLOWED = 1000
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


def parse_datetime_iso(raw):
    return datetime.fromisoformat(raw.replace('Z', '+00:00'))


def merge_tweet(tx, tweet):
    tx.run(
        "MERGE (t: Tweet {id: $id, text: $text, created_at: $created_at})",
        id=tweet.id,
        text=tweet.text,
        created_at=tweet.created_at.isoformat()
    )


def merge_hashtag(tx, tag):
    tx.run("MERGE (t: Hashtag {tag: $tag})", tag=tag)


def merge_tweet_hashtag(tx, tweet, hashtag):
    tx.run("MATCH (t: Tweet), (h: Hashtag) "
           "WHERE t.id=$tweet_id AND h.tag=$hashtag_tag "
           "MERGE (t)-[r:TAGGED_AS]->(h)",
           tweet_id=tweet.id,
           hashtag_tag=hashtag
           )


def destroy_everything_txn(tx):
    tx.run("MATCH (n) DETACH DELETE n")


def delete_oldest_tweets_txn(tx, limit):
    tx.run(
        "match (t: Tweet) "
        "with t,datetime(t.created_at) as date "
        "order by date "
        "limit $limit "
        "detach delete t",
        limit=limit)


def delete_hashtags_without_tweet_txn(tx):
    tx.run("match (h:Hashtag) "
           "where not (:Tweet)-[:TAGGED_AS]->(h) "
           "detach delete h")


def destroy_everything(driver):
    with driver.session() as session:
        session.write_transaction(destroy_everything_txn)
        print("Destroyed everything")


def store_tweet(driver, tweet):
    with driver.session() as session:
        session.write_transaction(merge_tweet, tweet)
        for h in tweet.hashtags:
            session.write_transaction(merge_hashtag, h)
            session.write_transaction(merge_tweet_hashtag, tweet, h)
        print(f" done")


def delete_oldest_tweets(driver, limit):
    with driver.session() as session:
        session.write_transaction(delete_oldest_tweets_txn, limit)
        session.write_transaction(delete_hashtags_without_tweet_txn)
        print(f"Deleted oldest {limit} tweets")


# match (t: Tweet) where datetime(t.created_at) > (datetime() - duration('PT1M')) return t
# match (t:Tweet)-[:TAGGED_AS]->(h:Hashtag) with count(*) as cnt, h order by cnt desc return h,cnt limit 10
# match (h:Hashtag)-[r:COTAGGED_WITH]-(h2:Hashtag) where r.cnt > 100 with h,h2,r order by r.cnt desc return h,r,h2,r.cnt limit 300

def delete_old_tweets(driver):
    with driver.session() as session:
        nodes_count = session.run("match (t) return count(*)").single()[
            'count(*)']
        print(f"current nodes count: {nodes_count}")
        if nodes_count > MAX_NODES_ALLOWED:
            nodes_to_clean = nodes_count - MAX_NODES_ALLOWED
            delete_oldest_tweets(driver, nodes_to_clean)


def main():
    with GraphDatabase.driver(graphenedb_url,
                              auth=(graphenedb_user, graphenedb_pass),
                              encrypted=True) as driver:

        # destroy_everything(driver)

        print("Starting Twitter stream ...")
        r = requests.get('https://api.twitter.com/2/tweets/sample/stream',
                         params={'tweet.fields': 'id,text,entities,created_at'},
                         headers={'Authorization': 'Bearer ' + twitter_bearer},
                         stream=True)

        if r.encoding is None:
            r.encoding = 'utf-8'
            print("Set encoding to utf-8")

        cnt = 0

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
                        tweet = Tweet(
                            id=parsed['data']['id'],
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


if __name__ == "__main__":
    main()
