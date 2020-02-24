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

logging.basicConfig(filename='errors.log', filemode='a+', format='%(asctime)s: %(message)s', level=logging.ERROR)
graph = Graph("bolt://localhost:7687", auth=("neo4j", "password"))


def dict_to_node(datadict, *labels, primarykey=None, primarylabel=None,):
    # if 'created_at' in datadict.keys():
    #     datadict['timestamp'] = get_timestamp(datadict['created_at'])
    cleandict = {}
    for key, value in datadict.items():
        if isinstance(datadict[key], np.int64):
            cleandict[key] = int(datadict[key])
        elif not isinstance(datadict[key],(int,str,float)):
            cleandict[key] = str(datadict[key])
        else:
            cleandict[key] = datadict[key]

    node = Node(*labels, **cleandict)
    node.__primarylabel__ = primarylabel or labels[0]
    node.__primarykey__ = primarykey
    return node


def hashtags_to_nodes(ents):
    """Returns list of Hashtag nodes"""
    out= []
    if ents['hashtags']:
        for each in ents['hashtags']:
            out.append(dict_to_node(each, 'Hashtag', primarykey='text', primarylabel='Hashtag'))
    return out


def mentions_to_nodes(ents):
    """Returns list of User nodes"""
    out=[]
    if ents['user_mentions']:
        for each in ents['user_mentions']:
            each.pop('indices')
            out.append(user_dtn(each))
    return out


def urls_to_nodes(ents):
    """Returns list of Url nodes"""
    out=[]
    if ents['urls']:
        for each in ents['urls']:
            each.pop('indices')
            out.append(dict_to_node(each, 'Url', primarykey='expanded_url', primarylabel='Url'))
    return out


def ent_parser(ents):
    """Returns dictionary of Hashtag, Mention and Url nodes"""
    output = {}
    dents = defaultdict(int)
    dents.update(ents)
    output['hashtags'] = hashtags_to_nodes(dents)
    output['mentions'] = mentions_to_nodes(dents)
    output['urls'] = urls_to_nodes(dents)
    return {k: v for (k, v) in output.items() if v}


def user_dtn(datadict):
    # if datadict['id'] in user_ids:
    #     return dict_to_node(datadict,'Target',primarykey='id',primarylabel='User',)
    return dict_to_node(datadict, 'User', primarykey='id', primarylabel='User')


def separate_children(tweet):
    try:
        retweeted = tweet.pop('retweeted_status')
    except KeyError:
        retweeted = []
    try:
        quoted = tweet.pop('quoted_status')
    except KeyError:
        quoted = []
    output = defaultdict(int)
    try:
        output['user'] = tweet.pop('user')
    except KeyError:
        output['user'] = []
    try:
        output['ents'] = tweet.pop('entities')
    except KeyError:
        output['ents'] = []
    output['tweet'] = dict(tweet)

    if isinstance(retweeted, dict) and isinstance(quoted, dict):
        retweeted.pop('quoted_status')
        output['qtuser'] = quoted.pop('user')
        output['qents'] = quoted.pop('entities')

        output['rtuser'] = retweeted.pop('user')
        output['rents'] = retweeted.pop('entities')
        output['retweeted'] = retweeted

        output['quoted'] = quoted

    elif isinstance(quoted, dict):
        output['qtuser'] = quoted.pop('user')
        output['qents'] = quoted.pop('entities')
        output['quoted'] = quoted

    elif isinstance(retweeted, dict):
        output['rtuser'] = retweeted.pop('user')
        output['rents'] = retweeted.pop('entities')
        output['retweeted'] = retweeted

    return output


def push_tweet(tweetdict):
    dicts = separate_children(tweetdict)
    tx = graph.begin()
    # cypher = graph.cypher

    if isinstance(dicts['user'], dict):
        user = user_dtn(dicts['user'])
    else:
        gaffer = user_dtn(dicts['tweet']['delete']['status'])
        regret = dict_to_node(dicts['tweet']['delete']['status'], 'Tweet')
        deleted = Relationship(gaffer, 'DELETES', regret, timestamp=dicts['tweet']['delete']['timestamp_ms'])
        tx.merge(gaffer, primary_key='id')
        tx.merge(regret, primary_key='id')
        tx.merge(deleted)
        tx.commit()
        return True
        
    tweet = dict_to_node(dicts['tweet'], 'Tweet')

    if 'retweeted' in dicts.keys():
        rtuser = user_dtn(dicts['rtuser'])
        retweet = dict_to_node(dicts['retweeted'],'Tweet')

        # Creates relationship U->U for a retweet
        graph.evaluate("MATCH (a:User {id:\'" + str(user['id']) + "\'}), (b:User {id:\'" + str(rtuser['id']) + "\'}) \
                    MERGE (a)-[r:RETWEETS]->(b) \
                    ON CREATE SET r.count = 1 \
                    WITH r \
                    CALL apoc.atomic.add(r, 'count', 1) YIELD newValue \
                    RETURN r")

        # Need to update table database with new stats from the time of the retweet

        tweeted2 = Relationship(rtuser,'TWEETS', retweet, timestamp=retweet['timestamp'],
                                created_at=retweet['created_at'], usrStatusCount=rtuser['statuses_count'],
                                usrFollowerCount=rtuser['followers_count'],
                                usrFavoritesCount=rtuser['favourites_count'])
        tx.merge(rtuser, primary_key='id')
        tx.merge(retweet, primary_key='id')
        tx.merge(tweeted2)
        for label, entities in ent_parser(dicts['rents']).items():
            # Goes through each entity and creates a relationship from the original tweet that contained it
            # and from the broadcasting User
            if entities:
                for entity in entities:
                    contains = Relationship(retweet, 'CONTAINS', entity)
                    tx.merge(entity, str(label), primary_key=entity.__primarykey__)
                    tx.merge(contains)
                    u_retweet = Relationship(rtuser, 'BROADCASTS', entity)
                    tx.merge(u_retweet)

    elif 'quoted' in dicts.keys():
        tweet.add_label('Qtweet')
        qtuser = user_dtn(dicts['qtuser'])
        quoted = dict_to_node(dicts['quoted'], 'Tweet')

        tweeted = Relationship(user, 'TWEETS', tweet, timestamp=tweet['timestamp'],
                               created_at=tweet['created_at'], usrStatusCount=user['statuses_count'],
                               usrFollowerCount=user['followers_count'], usrFavoritesCount=user['favourites_count'])

        tweeted2 = Relationship(qtuser, 'TWEETS', quoted, timestamp=quoted['timestamp'],
                                created_at=quoted['created_at'], usrStatusCount=qtuser['statuses_count'],
                                usrFollowerCount=qtuser['followers_count'],
                                usrFavoritesCount=qtuser['favourites_count'])

        quotes = Relationship(tweet, 'QUOTES', quoted, timestamp=tweet['timestamp'],
                                favcount=quoted['favourites_count'], replyCount=quoted['reply_count'],
                                sourceFollowers=qtuser['followers_count'], createdAt=tweet['created_at'],
                                retweetCount=quoted['retweet_count'], quoteCount=quoted['quote_count'])

        tx.merge(tweet, primary_key='id')
        tx.merge(user, primary_key='id')
        tx.merge(tweeted)
        tx.merge(qtuser, primary_key='id')
        tx.merge(quoted, primary_key='id')
        tx.merge(tweeted2)
        tx.merge(quotes)

        for label, entities in ent_parser(dicts['ents']).items():
            if entities:
                for entity in entities:
                    contains = Relationship(tweet, 'CONTAINS', entity)
                    tx.merge(entity, str(label), primary_key=entity.__primarykey__)
                    tx.merge(contains)

        for label, entities in ent_parser(dicts['qents']).items():
            if entities:
                for entity in entities:
                    contains = Relationship(quoted, 'CONTAINS', entity)
                    tx.merge(entity, str(label), primary_key=entity.__primarykey__)
                    tx.merge(contains)
        tx.commit()

    else:
        tweeted = Relationship(user, 'TWEETS', tweet, timestamp=tweet['timestamp'],
                               created_at=tweet['created_at'], usrStatusCount=user['statuses_count'],
                               usrFollowerCount=user['followers_count'], usrFavoritesCount=user['favourites_count'])
        tx.merge(tweet, primary_key='id')
        tx.merge(user, primary_key='id')
        tx.merge(tweeted)
        for label, entities in ent_parser(dicts['ents']).items():
            if entities:
                for entity in entities:
                    contains = Relationship(tweet, 'CONTAINS', entity)
                    tx.merge(entity, str(label), primary_key=entity.__primarykey__)
                    tx.merge(contains)
        tx.commit()
    return tweetdict['text']
    # tx.close()


def listen(status):
    try:
        full_text = push_tweet(status_to_dict(status))
        hash_tag = re.search(r'\#\w*', full_text)
        if hash_tag:
            if isinstance(hash_tag, list):
                hash_tags = hash_tag
            else:
                hash_tags = [hash_tag]
            return hash_tags
    except Exception as e:
        print(e)
        logging.error(f'Error on Listen: {e}\nFailed tweet: {status._json}')


class TwitterStreamListener(tweepy.StreamListener):
    """ A listener handles tweets are the received from the stream.
        Prints tweets received to terminal and to a new jsonl file every 10 minutes.
    """

    def on_status(self, status):
        from datetime import datetime
        rn = datetime.now()
        try:
            if 'extended_text' in status._json.keys():
                text = status.extended_text.full_text
            else:
                text = status.text
            print(f' ~~~ TweetTweetTweet from {status.user.screen_name}~~~ ')
            print(text+'\n\n')
        except:
            print('\n\nSIREN !?!?! Failure on TVStream !?!?! SIREN\n\n')
        with jsonlines.open('Data/Primary/Tweets-%s-%s-%s-%s.jsonl' %
                            (rn.month, rn.day, rn.hour, "{:02d}".format(floor(rn.minute/10)*10)), mode='a') as writer:
            writer.write(status_to_dict(status))

    def on_error(self, status_code):
        # self.cnt += 1
        print(f'Error being processed. Code: {status_code}') #, Cnt: {self.cnt}')
        if status_code == 420:
            print("The request is understood, but it has been refused or access is not allowed. Limit is maybe reached.")
        #    time.sleep(self.cnt << 1)
            return True
        else:
            logging.error(f'Status code: {status_code} in StreamListener')
            return False


def status_to_dict(tweet):
    try:
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
    tag_list = set(result['text'] for result in graph.run("""MATCH (n:Hashtag) RETURN n.text as text"""))
    watch_list = set(name_list+user_list).union(tag_list)

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
        matrix.close()
        sys.stdout.flush()
        os.execl('graphstream.py')

    # breakpoint()
    loop = asyncio.get_event_loop()
    tasks = [loop.create_task(cluster_flocks(RunTime, loop)),
             loop.create_task(myStream.filter(track=watch_list, is_async=False))]
    loop.run_until_complete(asyncio.wait(tasks))
    loop.close()
    sys.stdout.flush()
    os.execl('graphstream.py')

