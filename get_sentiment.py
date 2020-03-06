import tweepy
import config
from py2neo import Graph
import logging
import pandas as pd
import re
import nltk
from nltk import word_tokenize, FreqDist
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from nltk.collocations import BigramCollocationFinder as big_find
import string
from wordcloud import WordCloud
import matplotlib.pyplot as plt

# Set up logging, database connection, twitter streamer and NLTK
logging.basicConfig(filename='errors.log', filemode='a+', format='%(asctime)s: %(message)s', level=logging.ERROR)
graph = Graph("bolt://localhost:7687", auth=("neo4j", "password"))

auth = tweepy.OAuthHandler(config.consumer_key, config.consumer_secret)
auth.set_access_token(config.access_token, config.access_token_secret)
api = tweepy.API(auth, wait_on_rate_limit=True)

nltk.download('wordnet')
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
            tag (String): Hashtag being looked at
            top (int): Number of words to include in the WordCloud
    Returns:
        None: The output is a visualization of the strings in series in terms of the
            frequency of their occurrence.
    """
    vocab = tokenized(series)
    cloud = WordCloud(background_color='coral', max_words=top,  colormap='Blues')\
        .generate(' '.join([word for word in vocab]))
    plt.figure(figsize=(24, 12))
    plt.imshow(cloud, interpolation='bilinear')
    if tag:
        plt.title(f'Most Common words for {tag}')
    else:
        plt.title(f'Most Common Words', size='40', pad=20)
    plt.axis('off')
    plt.show();


def strip_tweets(tweet):
    """Process tweet text to remove retweets, mentions,links and hashtags."""
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
    """
    Run a Cypher query against the graph, put the results into a df

    Parameters
    ----------
    cypher : cypher query to be executed, may or may not have parameters to insert
    index_col : which column to use as the index, otherwise none used

    Returns
    -------
    df : a DataFrame
    """
    results = graph.run(cypher)
    resrows = [{'name': i[0], 'followers': i[1], 'text': i[2], 'timestamp': i[3]} for i in results]
    df = pd.DataFrame(resrows)
    if index_col is not None:
        if index_col =='timestamp':
            df.set_index(pd.to_datetime(df.timestamp)).drop(['timestamp'],axis=1)
        else:
            df.set_index(index_col).drop([index_col], axis=1)

    return df


def primary_species(labels, prop, weight=False):
    """Takes in a Label and returns the subgraph for that Label and a list of processed tweet text and property"""
    weight_clause = f", r.{weight} as {weight}"
    query = f""" MATCH (u:{labels[0]})-[r]-(t:{labels[1]}) {' WHERE EXISTS (r.'+f'{weight}) ' if weight else ''}
                 RETURN u.screen_name as name, u.followers_count as followers, t.{prop} as {prop}{weight_clause if weight else ''}
            """
    return query
