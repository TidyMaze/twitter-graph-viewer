import os

from neo4j import GraphDatabase

MAX_NODES_ALLOWED = 950

def get_neo4j_driver():
    graphenedb_url = os.environ.get("GRAPHENEDB_BOLT_URL")
    graphenedb_user = os.environ.get("GRAPHENEDB_BOLT_USER")
    graphenedb_pass = os.environ.get("GRAPHENEDB_BOLT_PASSWORD")
    return GraphDatabase.driver(graphenedb_url,
                                auth=(graphenedb_user, graphenedb_pass),
                                encrypted=True)

def get_all(driver):
    with driver.session() as session:
        top = session.run("match ()-[r:TAGGED_AS]->(h:Hashtag) with count(()-->(h)) as cnt, h order by cnt desc where cnt > 1 return h, cnt limit 30")
        top_tags = list(map(lambda record: record['h']['tag'], top))
        print(f"top is {top_tags}")
        r = session.run("match (t:Tweet)-[r:TAGGED_AS]->(h:Hashtag) where h.tag in $top_tags return r, t, h", top_tags = top_tags)
        return r.data()

def merge_tweet(tx, tweet):
    tx.run(
        "MERGE (t: Tweet {id: $id, text: $text, created_at: $created_at, username: $username})",
        id=tweet.id,
        text=tweet.text,
        created_at=tweet.created_at.isoformat(),
        username=tweet.username
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
