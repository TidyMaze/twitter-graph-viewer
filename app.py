from twitter_scraper import get_trends, get_tweets

for tweet in get_tweets('#france', pages=1):
	print(tweet['text'])
print(get_trends())