import numpy as np
from collections import defaultdict
from py2neo import Graph, Node, Relationship
import jsonlines
import re
import logging
from datetime import datetime
import os
from collections import Counter
import glob

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

# Functions to set up and encode sentiment

# nltk.download('vader_lexicon')
#
# sentiment_analyzer = SentimentIntensityAnalyzer()

#
# def polarity_scores(doc):
#     """Returns polarity score set earlier to Vader's analyzer"""
#     return sentiment_analyzer.polarity_scores(doc.text)
#
#
# def graph_sentiment(text):
#     tweet = nlp(strip_tweets(text))
#     return tweet._.polarity_scores['compound'], tweet.vector


# def encode_sentiment(tweet):
#     sentiment, embedding = graph_sentiment(tweet['text'])
#     sentiment = float(sentiment)
#     t_id = tweet['id_str']
#     if not isinstance(tweet['retweeted_status'], dict):
#         query = '''MERGE (t:Tweet {id_str: $id})
#         ON CREATE SET t.stranded = 1
#         ON MATCH SET t.sentiment = $sentiment,
#             t.embedding = $embedding
#         '''
#         graph.run(query, id=t_id, sentiment=sentiment, embedding=list(embedding))
#         print('Sentimental')


# Doc.set_extension('polarity_scores', getter=polarity_scores)

# Connect to local Neo4J DB
graph = Graph("bolt://localhost:7687", auth=("neo4j", "pa55w0rd"))


def dict_to_node(datadict, *labels, primarykey=None, primarylabel=None, ):
    """Take in a dictionary and return an instance of the Node class with associated properties"""
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

def media_to_nodes(ents):
    """Returns list of Media nodes"""
    out = []
    if ents['media']:
        for each in ents['media']:
            each.pop('indices')
            out.append(dict_to_node(each, 'Media', primarykey='expanded_url', primarylabel='Media'))
    return out
    


def ent_parser(ents):
    """Returns dictionary of Hashtag, Mention, Url and Media nodes for entity relationships"""
    output = {}
    dents = defaultdict(int)
    dents.update(ents)
    output['Hashtag'] = hashtags_to_nodes(dents)
    output['User'] = mentions_to_nodes(dents)
    output['Url'] = urls_to_nodes(dents)
    output['Media'] = media_to_nodes(dents)
    return {k: v for (k, v) in output.items() if v}


def user_dtn(datadict):
    """Return single User node"""
    return dict_to_node(datadict, 'User', primarykey='id', primarylabel='User')


def separate_children(tweet):
    """Take tweet dict and separate into separate Tweet, User and entity dictionaries contained in output dict"""
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
        # Case where tweet both retweeted and quoted
        retweeted.pop('quoted_status')
        output['qtuser'] = quoted.pop('user')
        output['qents'] = quoted.pop('entities')

        output['rtuser'] = retweeted.pop('user')
        output['rents'] = retweeted.pop('entities')
        output['retweeted'] = retweeted

        output['quoted'] = quoted

    elif isinstance(quoted, dict):
        # Case where tweet was quote
        output['qtuser'] = quoted.pop('user')
        output['qents'] = quoted.pop('entities')
        output['quoted'] = quoted

    elif isinstance(retweeted, dict):
        # Case where tweet was retweet
        output['rtuser'] = retweeted.pop('user')
        output['rents'] = retweeted.pop('entities')
        output['retweeted'] = retweeted

    return output


def push_tweet(tweetdict):
    """Take tweet dict and create Nodes and Relationships to be pushed into network DB"""
    try:
        # Separate into various dictionaries
        dicts = separate_children(tweetdict)
        # Start transaction
        tx = graph.begin()

        # Handles case where tweet was deleted (may be deprecated)
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

        # Create tweet Node
        tweet = dict_to_node(dicts['tweet'], 'Tweet')

        # Handle retweet relationships
        if 'retweeted' in dicts.keys():
            rtuser = user_dtn(dicts['rtuser'])
            retweet = dict_to_node(dicts['retweeted'], 'Tweet')
            tweeted2 = Relationship(rtuser, 'TWEETS', retweet, timestamp=retweet['timestamp'],
                                    created_at=retweet['created_at'], usrStatusCount=rtuser['statuses_count'],
                                    usrFollowerCount=rtuser['followers_count'],
                                    usrFavoritesCount=rtuser['favourites_count'])
            tx.merge(user, primary_key='id')
            tx.merge(rtuser, primary_key='id')
            tx.merge(retweet, primary_key='id')
            tx.merge(tweeted2)
            tx.commit()
            # Creates relationship U->U for a retweet
            graph.evaluate("MATCH (a:User {id: " + str(user['id']) + "}) \
                            WITH a \
                            MATCH (b:User {id: " + str(rtuser['id']) + "}) \
                            MERGE (a)-[r:RETWEETS]->(b) \
                            ON CREATE SET r.count = 1 \
                            ON MATCH SET r.count = r.count+1")

            # Need to update table database with new stats from the time of the retweet

            for label, entities in ent_parser(dicts['rents']).items():
                # Goes through each entity and creates a relationship from the original tweet that contained it
                # and from the broadcasting User
                if entities:
                    for entity in entities:
                        contains = Relationship(retweet, 'CONTAINS', entity)
                        tx.merge(entity, str(label), primary_key=entity.__primarykey__)
                        tx.merge(contains)
                        if label == 'User':
                            query = "MATCH (a:User {id: " + str(rtuser['id']) + "}) " + \
                                    "WITH a " + \
                                    "MATCH (b:User {id: '" + str(entity['id']) + "'}) \
                                        MERGE (a)-[r:BROADCASTS]->(b) \
                                        ON CREATE SET r.count = 1 \
                                        ON MATCH SET r.count = r.count+1"
                            graph.evaluate(query)
                        elif label == 'Hashtag':
                            query = "MATCH (b:Hashtag {text: '" + entity['text'] + "'}) " + \
                                    "WITH b " + \
                                    "MATCH (a:User {id: " + str(rtuser['id']) + "})  \
                                        MERGE (a)-[r:BROADCASTS]->(b) \
                                        ON CREATE SET r.count = 1 \
                                        ON MATCH SET r.count = r.count+1"
                            graph.evaluate(query)
            tx.commit()

        # Handle quoted relationships
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
            tx.commit()

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

        # Handle normal tweet relationship
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
    except Exception as e:
        logging.error(f'Error on push: {e}. Tweet: \n {tweetdict}')
        raise


def listen(status):
    """"""
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


if __name__ == "__main__":
    rn = datetime.now()
    RunTime = (datetime.now().minute/10-1)*10
    path = 'Data/BLM/'
    tags = Counter()
    # Loop through jsonl files in the above path
    list_of_files = glob.glob(os.path.join(path, '*.jsonl'))
    latest_file = max(list_of_files, key=os.path.getctime)
    for filename in list_of_files:
        print(f'{filename} being processed.')
        if filename != latest_file:
            with jsonlines.open(filename, mode='r') as reader:
                for line in reader:
                    try:
                        if re.match(rf'Data/BLM/Tweets-{rn.month}-{rn.day}-{rn.hour}.*', filename):
                            recent = listen(line)
                            if recent:
                                tags.update(recent)
                        else:
                            listen(line)
                    except Exception as e:
                        logging.error(f'Error on Read: {e}\nFailed tweet: {line}')
                print(f'{filename} processed in {datetime.now()-rn} seconds.')
            # shutil.move(filename, filename[:12]+'_Processed'+filename[12:])
#     with open(f'Data/Tags/{rn.month}-{rn.day}-{rn.hour}.txt', 'w') as f:
#         for tag in tags.most_common(10):
# #             f.write(tag[0]+'\n')
#     print(f'~~~~{datetime.now()}~~~~')
#     print(f"Tags from listening: {tags.most_common(10)}\n")
