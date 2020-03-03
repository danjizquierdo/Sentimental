import tweepy
import json
import config
import datetime
import re
from py2neo import Graph
import logging
from sys import argv
import pandas as pd
import re
import nltk
from nltk import word_tokenize, FreqDist
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from nltk.collocations import BigramCollocationFinder as big_find
import string
from wordcloud import WordCloud
from PIL import Image
import matplotlib.pyplot as plt
from collections import Counter
import numpy as np
import urllib
import requests
from random import randint


logging.basicConfig(filename='errors.log', filemode='a+', format='%(asctime)s: %(message)s', level=logging.ERROR)
graph = Graph("bolt://localhost:7687", auth=("neo4j", "password"))

auth = tweepy.OAuthHandler(config.consumer_key, config.consumer_secret)
auth.set_access_token(config.access_token, config.access_token_secret)
api = tweepy.API(auth, wait_on_rate_limit=True)

# nlp = spacy.load("en_core_web_sm")
nltk.download('wordnet')

tweets = {}
users = {}

mask = np.array(Image.open(requests.get(
    'https://www.nicepng.com/png/full/73-737405_us-political-map-grayscale-united-states-map-gray.png',
    stream=True).raw))
# mask2 = np.array(Image.open(requests.get(
#     'https://www.vectorportal.com/img_novi/kangaroo-silhouette.jpg',
#     stream=True).raw))
lemmatizer = WordNetLemmatizer()


def process_tweet(tweet):
    """ Takes in a string, returns a list of words in the string that aren't stopwords
    Parameters:
        tweet (string):  string of text to be tokenized
    Returns:
        stopwords_removed (list): list of all words in tweet, not including stopwords
    """
    stopwords_list=stopwords.words('english') +list(string.punctuation)
    stopwords_list += ["'",'"','...','``','…','’','‘','“',"''",'""','”','”','co',"'s'",'\'s','n\'t','\'m','\'re','amp','https']
    tokens = nltk.word_tokenize(tweet)
    stopwords_removed = [lemmatizer.lemmatize(token).lower() for token in tokens if token not in stopwords_list]
    return stopwords_removed


def tokenized(series):
    """ Takes in a series containing strings or lists of strings, and creates a single list of all the words
    Parameters:
        series (series): series of text in the form of strings or lists of string

    Returns:
        tokens (list): list of every word in the series, not including stopwords
    """

    corpus = ' '.join(
        [tweet.lower() if type(tweet) == str else ' '.join([tag.lower() for tag in tweet]) for tweet in series])
    corpus, tags = strip_tweets(corpus)
    tokens = process_tweet(corpus)
    return tokens


def wordfrequency(series, top):
    """ Returns the frequency of words in a list of strings.
    Parameters:
        series (iterable): List of strings to be combined and analyzed
        top (int): The number of top words to return.
    Returns:
        list (tuples): List of word and value pairs for the top words in the series.
    """
    vocab = tokenized(series)
    big = big_find.from_words(vocab)
    big_measures = nltk.collocations.BigramAssocMeasures()
    bestBigrams = big.above_score(score_fn=big_measures.raw_freq, min_score=1.0 / len(tuple(nltk.bigrams(series))))
    vocab += bestBigrams
    frequencies = FreqDist(tokenized(series))
    return frequencies.most_common(top)


def create_wordcloud(series, tag=False, top=200):
    """ Take in a list of lists and create a WordCloud visualization for those terms.
    Parameters:
            series (iterable): A list of lists containing strings.
    Returns:
        None: The output is a visualization of the strings in series in terms of the
            frequency of their occurrence.
    """
    vocab = tokenized(series)
    cloud = WordCloud(background_color='whitesmoke', max_words=top, mask=mask, width=400, height=300,
                      contour_width=3, contour_color='crimson').generate(' '.join([word for word in vocab]))
    plt.figure(figsize=(24, 12))
    plt.imshow(cloud, interpolation='bilinear')
    if tag:
        plt.title(f'Most Common words for {tag}')
    else:
        plt.title(f'Most Common Words')
    plt.axis('off')
    # plt.tight_layout(pad=0)
    plt.show();


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
    hashtags = re.findall(hashtag, tweet)
    tweet = re.sub(hashtag, '', tweet)
    return tweet, hashtags


def read_cypher(cypher, index_col=None):
    '''
    Run a Cypher query against the graph, put the results into a df

    Parameters
    ----------
    cypher : cypher query to be executed, may or may not have parameters to insert
    index_col : which column to use as the index, otherwise none used

    Returns
    -------
    df : a DataFrame
    '''
    results = graph.run(cypher)
    resrows = [{'name': i[0], 'followers': i[1], 'text': i[2], 'timestamp': i[3]} for i in results]
    df = pd.DataFrame(resrows)
    if index_col != None:
        if index_col =='timestamp':
            df.set_index(pd.to_datetime(df.timestamp)).drop(['timestamp'],axis=1)
        else:
            df.set_index(index_col).drop([index_col], axis=1)


    #     if parse_dates != None:
    #         if isinstance(parse_dates, basestring):
    #             df[parse_dates] = to_datetime(df[parse_dates], unit = 's')
    #         elif type(parse_dates) is list:
    #             for col in parse_dates:
    #                 df[col] = to_datetime(df[col], unit = 's')
    return df


def primary_species(labels, prop, weight=False):
    """Takes in a Label and returns the subgraph for that Label and a list of processed tweet text and property"""
    weight_clause = f", r.{weight} as {weight}"
    query = f""" MATCH (u:{labels[0]})-[r]-(t:{labels[1]}) {' WHERE EXISTS (r.'+f'{weight}) ' if weight else ''}
                 RETURN u.screen_name as name, u.followers_count as followers, t.{prop} as {prop}{weight_clause if weight else ''}
            """
    return query


def attend_rallies(df):
    #Australia project
    """Takes in a dataframe and returns the same dataframe with a cleaned text and hashtag column per tweet"""
    df[['clean_text', 'hashtag']] = df.text.apply(strip_tweets)
    return df


def cluster_flocks(dicts):
    #Australia project
    """Takes in a dictionary of dictionaries and returns a processed text value and hashtag count"""
    # hashtags = Counter()
    tweets = []
    for dic in dicts.values():
        text, tag = strip_tweets(dic['text'])
        hashtags.update(tag)
        tweets.append([text, tag])
    return tweets, hashtags


def myconverter(o):
    if isinstance(o, datetime.datetime):
        return o.__str__()


def listen(terms, amount):
    for term in terms:
        for tweet in tweepy.Cursor(api.search, q=term, count=amount, tweet_mode ='extended').items(amount):
            if (not tweet.retweeted) and ('RT @' not in tweet.full_text):
                try:
                    tweet_ = dict()
                    tweet_['created_at'] = tweet.created_at
                    tweet_['text'] = tweet.full_text
                    if tweet.lang:
                        tweet_['lang'] = tweet.lang
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
                    hash_tag = re.search(r'\#\w*',tweet.full_text)
                    if hash_tag:
                        if isinstance(hash_tag,list):
                            # for tag in hash_tag: hash_tags.add(tag)
                            tweet_['hashtags']= hash_tag
                        else:
                            # hash_tags.add(hash_tag)
                            tweet_['hashtags']= [hash_tag]
                    tweets[int(tweet.id)] = tweet_
                except Exception as e:
                    print(e)
                    print(tweet)
                    logging.error(f'Error: {e}\nFailed term: {term}Failed tweet: {tweet}')
                    continue

                try:
                    user_ = dict()
                    user_['screen_name'] = tweet.user.screen_name
                    user_['followers_count'] = tweet.user.followers_count
                    user_['verified'] = tweet.user.verified
                    user_['created_at'] = tweet.user.created_at
                    if tweet.user.lang:
                        user_['lang'] = tweet.user.lang
                    users[int(tweet.user.id)] = user_
                except Exception as e:
                    print(e)
                    print(tweet)
                    logging.error(f'Error: {e}\nFailed term: {term} Failed user: {user}\n')
                    continue
        print(f'Done with {term}. Currently {len(tweets)} collected from {len(users)} users.')

    print(f'{len(tweets)} tweets and {len(users)} users.')
    with open('tweets.json', 'a+') as t:
        json.dump(tweets, t, default=myconverter)
    with open('users.json', 'a+') as u:
        json.dump(users, u, default=myconverter)

    # for hash_tag in hash_tags:
    #     for tweet in tweepy.Cursor(api.search, q=hash_tag, count=1000).items(1000):
    #         if (not tweet.retweeted) and ('RT @' not in tweet.text):
    #             # if tweet.lang == "en":
    #                 twitter_users.append(tweet.user.name)
    #                 tweet_time.append(tweet.created_at)
    #                 tweet_string.append(tweet.text)
    #                 if hash_tag:
    #                     if isinstance(hash_tag, list):
    #                         for tag in hash_tag: hash_tags.add(tag)
    #                     else:
    #                         hash_tags.add(hash_tag)


if __name__ == "__main__":
    listen([
        '#Australia', '#AustralianFires', '#koala', '#AustraliaBurning',
        '#ClimateActionNow', '#AustraliaBushFires', '#bushfirecrisis', '#canberra',
        '#auspol', '#koalateelove', '#aussiemateship', '#Illridewithyou', '#sydneysmoke',
        '#sydneyfires', '#nswfires', '#climatecrisis', '#straya', 'brushfire',
        '#canberrasmoke', '#canberrafires', '#AustraliaBurns', '#namadgi'
    ], 1000)
