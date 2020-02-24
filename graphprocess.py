import pandas as pd
import numpy as np
from collections import defaultdict
from py2neo import Graph, Node, Relationship
import jsonlines
import spacy
from spacy.tokens import Doc
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import nltk
import re
import logging
from datetime import datetime
import os
from math import floor
import shutil

logging.basicConfig(filename='neo4j_errors.log', filemode='a+', format='%(asctime)s: %(message)s', level=logging.ERROR)


def strip_tweets(tweet):
    '''Process tweet text to remove retweets, mentions,links and hashtags.'''
    retweet = r'RT:? ?@\w+:?'
    tweet = re.sub(retweet, '', tweet)
    mention = r'@\w+'
    tweet = re.sub(mention, '', tweet)
    links = r'^(http:\/\/www\.|https:\/\/www\.|http:\/\/|https:\/\/)?[a-z0-9]+([\-\.]{1}[a-z0-9]+)*\.[a-z]{2,5}(:[0-9]{1,5})?(\/.*)?$'
    tweet = re.sub(links, '', tweet)
    tweet_links = r'https:\/\/t\.co\/\w+|http:\/\/t\.co\/\w+'
    tweet = re.sub(tweet_links, '', tweet)
    tweet_link = r'http\S+'
    tweet = re.sub(tweet_link, '', tweet)
    hashtag = r'#\w+'
    tweet = re.sub(hashtag, '', tweet)
    return tweet


nltk.download('vader_lexicon')

sentiment_analyzer = SentimentIntensityAnalyzer()


def polarity_scores(doc):
    """Returns polarity score set earlier to Vader's analyzer"""
    return sentiment_analyzer.polarity_scores(doc.text)


def graph_sentiment(text):
    tweet = nlp(strip_tweets(text))
    return tweet._.polarity_scores['compound'], tweet.vector


def encode_sentiment(tweet):
    sentiment, embedding = graph_sentiment(tweet['text'])
    sentiment = float(sentiment)
    t_id = tweet['id_str']
    if not isinstance(tweet['retweeted_status'], dict):
        query = '''MERGE (t:Tweet {id_str: $id})
        ON CREATE SET t.stranded = 1 
        ON MATCH SET t.sentiment = $sentiment,
            t.embedding = $embedding
        '''
        graph.run(query, id=t_id, sentiment=sentiment, embedding=list(embedding))
        print('Sentimental')


Doc.set_extension('polarity_scores', getter=polarity_scores)

graph = Graph("bolt://localhost:7687", auth=("neo4j", "password"))


def get_timestamp(dt_ish):
    """Returns DateTime value"""
    if isinstance(dt_ish, str):
        return pd.to_datetime(dt_ish).timestamp()
    else:
        return dt_ish.timestamp()


def dict_to_node(datadict, *labels, primarykey=None, primarylabel=None, ):
    # if 'created_at' in datadict.keys():
    #     datadict['timestamp'] = get_timestamp(datadict['created_at'])
    cleandict = {}
    for key, value in datadict.items():
        if isinstance(datadict[key], np.int64):
            cleandict[key] = int(datadict[key])
        elif not isinstance(datadict[key], (int, str, float)):
            cleandict[key] = str(datadict[key])
        else:
            cleandict[key] = datadict[key]

    node = Node(*labels, **cleandict)
    node.__primarylabel__ = primarylabel or labels[0]
    node.__primarykey__ = primarykey
    return node


def hashtags_to_nodes(ents):
    """Returns list of Hashtag nodes"""
    out = []
    if ents['hashtags']:
        for each in ents['hashtags']:
            out.append(dict_to_node(each, 'Hashtag', primarykey='text', primarylabel='Hashtag'))
    return out


def mentions_to_nodes(ents):
    """Returns list of User nodes"""
    out = []
    if ents['user_mentions']:
        for each in ents['user_mentions']:
            each.pop('indices')
            out.append(user_dtn(each))
    return out


def urls_to_nodes(ents):
    """Returns list of Url nodes"""
    out = []
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
        retweet = dict_to_node(dicts['retweeted'], 'Tweet')

        # Creates relationship U->U for a retweet
        graph.evaluate("MATCH (a:User {id:\'" + str(user['id']) + "\'}), (b:User {id:\'" + str(rtuser['id']) + "\'}) \
                    MERGE (a)-[r:RETWEETS]->(b) \
                    ON CREATE SET r.count = 1 \
                    WITH r \
                    CALL apoc.atomic.add(r, 'count', 1) YIELD newValue \
                    RETURN r")

        # Need to update table database with new stats from the time of the retweet

        tweeted2 = Relationship(rtuser, 'TWEETS', retweet, timestamp=retweet['timestamp'],
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


def listen(status):
    try:
        full_text = push_tweet(status)
        hash_tag = re.search(r'\#\w*', full_text)
        if hash_tag:
            if isinstance(hash_tag, list):
                hash_tags = hash_tag
            else:
                hash_tags = [hash_tag]
            return hash_tags
        return []
    except Exception as e:
        print(e)
        logging.error(f'Error on Listen: {e}\nFailed tweet: {status}')


# def status_to_dict(tweet):
#     try:
#         tweet_ = dict()
#         tweet_['created_at'] = tweet.created_at
#         tweet_['text'] = tweet.full_text
#         if tweet.lang:
#             tweet_['lang'] = tweet.lang
#         if 'retweeted_status' in tweet._json.keys():
#             tweet_['retweeted_status'] = status_to_dict(tweet.retweeted_status)
#         if 'quoted_status' in tweet._json.keys():
#             tweet_['quoted_status'] = status_to_dict(tweet.quoted_status)
#         if tweet.in_reply_to_status_id:
#             tweet_['in_reply_to_status_id'] = tweet.in_reply_to_status_id
#         if tweet.in_reply_to_user_id:
#             tweet_['in_reply_to_user_id'] = tweet.in_reply_to_user_id
#         if tweet.retweet_count:
#             tweet_['retweet_count'] = tweet.retweet_count
#         else:
#             tweet_['retweet_count'] = 0
#         if tweet.favorite_count:
#             tweet_['favorite_count'] = tweet.favorite_count
#         else:
#             tweet_['favorite_count'] = 0
#         tweet_['entities'] = tweet.entities
#         tweet_['user_id'] = tweet.user.id
#         tweet_['coordinates'] = tweet.coordinates
#         # hash_tag = re.search(r'\#\w*', tweet.full_text)
#         # if hash_tag:
#         #     if isinstance(hash_tag, list):
#         #         # for tag in hash_tag: hash_tags.add(tag)
#         #         tweet_['hashtags'] = hash_tag
#         #     else:
#         #         # hash_tags.add(hash_tag)
#         #         tweet_['hashtags'] = [hash_tag]
#         tweet_['id'] = int(tweet.id)
#     except Exception as e:
#         print(e)
#         print(tweet)
#         logging.error(f'Error: {e}\nFailed tweet: {tweet}\n')
#
#     try:
#         user = dict()
#         user['screen_name'] = tweet.user.screen_name
#         user['followers_count'] = tweet.user.followers_count
#         user['verified'] = tweet.user.verified
#         user['created_at'] = tweet.user.created_at
#         user['id'] = tweet.user.id
#         if tweet.user.lang:
#             user['lang'] = tweet.user.lang
#         tweet_['user'] = user
#     except Exception as e:
#         print(e)
#         print(tweet)
#         logging.error(f'Error: {e}\nFailed tweet: {tweet}\n')
#     return tweet_


if __name__ == "__main__":
    rn = datetime.now()
    RunTime = (datetime.now().minute/10-1)*10
    path = 'Data/Primary/'
    tags = []
    for filename in os.listdir('Data/Primary/'):

        if filename != 'Tweets-%s-%s-%s-%s.jsonl'.format(
                rn.month, rn.day, rn.hour, "{:02d}".format(floor(rn.minute / 10) * 10)):
            with jsonlines.open(path+filename, mode='r') as reader:
                for line in reader:
                    try:
                        tags += listen(line)
                    except Exception as e:
                        logging.error(f'Error on Read: {e}\nFailed tweet: {line}')
                        break
                print(f'{filename} processed in {datetime.now()-rn} seconds.')
            # Move a file from the directory d1 to d2
            shutil.move(path+filename, 'Data/Primary_Processed/'+filename)

    logging.debug(f'~~~~{datetime.now*()}~~~~')
    logging.debug(f"Tags from listening: {tags}\n")
    logging.debug(f'Tags from querying: {[print(tag) for tag in graph.execute("""MATCH (n:Hashtag) RETURN n.text""")]}\n\n')
