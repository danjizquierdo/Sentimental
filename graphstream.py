from py2neo import Graph
import jsonlines
import tweepy
import logging
import config
import sys
from datetime import datetime
import time
from math import floor
import asyncio
import os
import glob
import urllib3

logging.basicConfig(filename='errors.log', filemode='a+', format='%(asctime)s: %(message)s', level=logging.ERROR)
graph = Graph("bolt://localhost:7687", auth=("neo4j", "password"))


class TwitterStreamListener(tweepy.StreamListener):
    """ A listener handles tweets as they are received from the stream.
        Prints tweets received to terminal and to a new jsonl file every 10 minutes.
    """

    def on_status(self, status):
        from datetime import datetime
        rn = datetime.now()
        if rn.second % 5 == 0:
            try:
                if 'extended_tweet' in status._json.keys():
                    text = status.extended_tweet.full_text
                else:
                    text = status.text
                print(f' ~~~ TweetTweetTweet from {status.user.screen_name}~~~ ')
                print(text+'\n\n')
            except:
                print('\n\nSIREN !?!?! Failure on TVStream !?!?! SIREN\n\n')
        with jsonlines.open('Data/Primary/Tweets-%s-%s-%s-%s.jsonl' %
                            (rn.month, rn.day, rn.hour, "{:02d}".format(floor(rn.minute/10)*10)), mode='a') as writer:
            tweet = status_to_dict(status)
            if tweet:
                writer.write(tweet)

    def on_error(self, status_code):
        from datetime import datetime
        print(f'Error being processed. Code: {status_code}')
        if status_code == 420:
            logging.error(f"{datetime.now()}: The request is understood, but it has been refused or access is not allowed. Limit is maybe reached.\n")
            return True
        else:
            logging.error(f'{datetime.now()} Status code: {status_code} in StreamListener.\n')
            return True


def status_to_dict(tweet):
    try:
        if tweet.lang == 'in':
            return None
        tweet_ = dict()
        tweet_['timestamp'] = tweet.created_at.timestamp()
        if 'extended_text' in tweet._json.keys():
            tweet_['text'] = tweet.extended_text.full_text
            if 'extended_entities' in tweet.extended_text._json.keys():
                tweet_['entities'] = tweet.extended_text.entities
            else:
                tweet_['entities'] = tweet.extended_text.entities
        else:
            tweet_['text'] = tweet.text
            tweet_['entities'] = tweet.entities
        if tweet.lang:
            tweet_['lang'] = tweet.lang
        if 'retweeted_status' in tweet._json.keys():
            tweet_['retweeted_status'] = status_to_dict(tweet.retweeted_status)
        if 'quoted_status' in tweet._json.keys():
            tweet_['quoted_status'] = status_to_dict(tweet.quoted_status)
        if tweet.in_reply_to_status_id:
            tweet_['in_reply_to_status_id'] = tweet.in_reply_to_status_id
        if tweet.in_reply_to_user_id:
            tweet_['in_reply_to_user_id'] = tweet.in_reply_to_user_id
        if tweet.retweet_count:
            tweet_['retweet_count'] = tweet.retweet_count
        else:
            tweet_['retweet_count'] = 0
        if tweet.favorite_count:
            tweet_['favorite_count'] = tweet.favorite_count
        else:
            tweet_['favorite_count'] = 0
        tweet_['user_id'] = tweet.user.id
        tweet_['coordinates'] = tweet.coordinates
        tweet_['id'] = int(tweet.id)
    except Exception as e:
        print(e)
        logging.error(f'Error on status_to_dict[Tweet]: {e}\nFailed tweet: {tweet._json}\n')

    try:
        user = dict()
        user['screen_name'] = tweet.user.screen_name
        user['followers_count'] = tweet.user.followers_count
        user['verified'] = tweet.user.verified
        user['created_at'] = tweet.user.created_at.timestamp()
        user['id'] = tweet.user.id
        if tweet.user.lang:
            user['lang'] = tweet.user.lang
        tweet_['user'] = user
    except Exception as e:
        print(e)
        logging.error(f'Error on status_to_dict[User]: {e}\nFailed tweet: {tweet._json}\n')
        tweet_['user'] = None
    return tweet_


if __name__ == "__main__":
    #Construct watch list from names, usernames and seen hashtags
    name_list = ['Joe Biden', 'Bernie Sanders', 'Elizabeth Warren', 'Amy Klobuchar', 'Michael Bloomberg',
                'Andrew Yang', 'Tulsi Gabbard', 'Pete Buttigieg']
    name_list += [name.split()[1] for name in name_list]
    name_list += ['Bernie']
    user_list = ['JoeBiden', 'BernieSanders', 'ewarren', 'amyklobuchar', 'MikeBloomberg', 'AndrewYang',
                 'TulsiGabbard', 'PeteButtigieg']
    name_list += ["@" + name for name in user_list]
    user_ids = ['939091', '216776631', '357606935', '33537967', '16581604', '2228878592', '26637348', '226222147']
    watch_list = name_list+user_list

    # Eventually want to add in dynamic detection of trending hashtags based on last hour's activity
    # list_of_files = glob.glob('/Data/Tag/*.txt')
    # if list_of_files:
    #     latest_file = max(list_of_files, key=os.path.getctime)
    #     with open(latest_file, 'r') as f:
    #         watch_list += [tag for tag in f.readlines()]
    # tag_list = set(result['text'] for result in graph.run("""MATCH (n:Hashtag) RETURN n.text as text"""))
    # watch_list = set(name_list+user_list)#.union(tag_list)

    #Set up Tweepy Stream
    auth = tweepy.OAuthHandler(config.consumer_key, config.consumer_secret)
    auth.set_access_token(config.access_token, config.access_token_secret)
    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True, retry_count=10, retry_delay=5,
                     retry_errors=5)
    myStreamListener = TwitterStreamListener()
    myStream = tweepy.Stream(auth=api.auth, listener=myStreamListener)

    #Start your engines
    dt = datetime.now()
    RunTime = datetime.now().minute/10*10


    async def cluster_flocks(start, matrix):
        while datetime.now().minute/10*10 == start:
            await time.sleep(1)
        print('\\m/ Flocks clustered. Time to jack out and start again. \\m/\n')
        matrix.close()
        sys.stdout.flush()
        os.execl('graphstream.py')

    # breakpoint()
    loop = asyncio.get_event_loop()
    tasks = [loop.create_task(cluster_flocks(RunTime, loop)),
             loop.create_task(myStream.filter(track=watch_list, languages=['en'], is_async=False))]
    try:
        loop.run_until_complete(asyncio.wait(tasks))
    except urllib3.exceptions.ProtocolError as e:
        loop.close()
        logging.error(f'{datetime.now()}: Stream has ended with error "{e}"')
        print(f'\n\nRestarting stream in 10 seconds.\n\n')
        time.wait(10)
        sys.stdout.flush()
        os.execl('graphstream.py')

