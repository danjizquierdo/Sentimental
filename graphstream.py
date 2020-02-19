import pandas as pd
import numpy as np
from pprint import pprint
from collections import defaultdict
from py2neo import Graph, Node, Relationship
import json
import time
# from IPython.display import clear_output
import spacy
from spacy.tokens import Doc
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import nltk
import re

def strip_tweets(tweet):
    '''Process tweet text to remove retweets, mentions,links and hashtags.'''
    retweet = r'RT:? ?@\w+:?'
    tweet= re.sub(retweet,'',tweet)
    mention = r'@\w+'
    tweet= re.sub(mention,'',tweet)
    links = r'^(http:\/\/www\.|https:\/\/www\.|http:\/\/|https:\/\/)?[a-z0-9]+([\-\.]{1}[a-z0-9]+)*\.[a-z]{2,5}(:[0-9]{1,5})?(\/.*)?$'
    tweet= re.sub(links,'',tweet)
    tweet_links = r'https:\/\/t\.co\/\w+|http:\/\/t\.co\/\w+'
    tweet=re.sub(tweet_links,'',tweet)  
    tweet_link = r'http\S+'
    tweet=re.sub(tweet_link,'',tweet)
    hashtag = r'#\w+'
    tweet= re.sub(hashtag,'',tweet)
    return tweet

nltk.download('vader_lexicon')

sentiment_analyzer = SentimentIntensityAnalyzer()

def migrate_flocks(flocks):
    cnt=0
    start=time.time()
    for flock in flocks:
        cnt+=1
        for row in range(len(flock)):
            if row%25 ==0:
                d_bird = dict(flock.iloc[row])
                push_tweet(d_bird)
                print('Tweet!')
        clear_output()
        print(f'Time since migration: {time.time()-start} seconds' )
        print(f'{cnt} batches parsed!')

def polarity_scores(doc):
    return sentiment_analyzer.polarity_scores(doc.text)

def graph_sentiment(text):
    tweet = nlp(strip_tweets(text))
    return tweet._.polarity_scores['compound'],tweet.vector

def encode_sentiment(tweet):
    sentiment,embedding = graph_sentiment(tweet['text'])
    sentiment=float(sentiment)
    t_id=tweet['id_str']
    if not isinstance(tweet['retweeted_status'],dict):
        query = '''MERGE (t:Tweet {id_str: $id})
        ON CREATE SET t.stranded = 1 
        ON MATCH SET t.sentiment = $sentiment,
            t.embedding = $embedding
        '''
        graph.run(query,id=t_id,sentiment=sentiment,embedding=list(embedding))
        print('Sentimental')


Doc.set_extension('polarity_scores', getter=polarity_scores)

graph = Graph("bolt://localhost:7687", auth=("neo4j", "password")) 

tag_list = ['Joe Biden','Bernie Sanders','Kamala Harris', 'Cory Booker',
'Elizabeth Warren',"Beto O'Rourke","Beto ORourke",'Eric Holder','Sherrod Brown',
'Amy Klobuchar','Michael Bloomberg','John Hickenlooper','Kirsten Gillibrand',
'Andrew Yang','Julian Castro','Julián Castro','Eric Swalwell','Tulsi Gabbard',
'Jay Inslee','Pete Buttigieg', 'John Delaney','Mike Gravel','Wayne Messam',
'Tim Ryan','Marianne Willamson','Stacy Abrams','Mayor Pete']


user_list = ['JoeBiden','BernieSanders','KamalaHarris','CoryBooker',
'ewarren','BetoORourke','EricHolder','SherrodBrown','amyklobuchar',
'MikeBloomberg','Hickenlooper','SenGillibrand','AndrewYang','JulianCastro',
'ericswalwell','TulsiGabbard','JayInslee','PeteButtigieg','JohnDelaney',
'MikeGravel','WayneMessam','TimRyan','marwilliamson','staceyabrams']

tag_list += ["@"+name for name in user_list]


user_ids = ['939091','216776631','30354991','15808765','357606935','342863309','3333055535',
'24768753','33537967','16581604','117839957','72198806','2228878592','19682187','377609596',
'26637348','21789463','226222147','426028646','14709326','33954145','466532637','21522338',
'216065430']

def get_timestamp(dt_ish):
    if isinstance(dt_ish,str):
        return pd.to_datetime(dt_ish).timestamp()
    else:
        return dt_ish.timestamp()


def dict_to_node(datadict,*labels,primarykey=None,primarylabel=None,):
    if 'created_at' in datadict:
        datadict['timestamp']=get_timestamp(datadict['created_at'])
    cleandict={}
    for key,value in datadict.items():
        if isinstance(datadict[key],np.int64):
            cleandict[key] = int(datadict[key])
        elif not isinstance(datadict[key],(int,str,float)):
            cleandict[key] = str(datadict[key])
        else:
            cleandict[key] = datadict[key]

    node = Node(*labels,**cleandict)
    node.__primarylabel__= primarylabel or labels[0]
    node.__primarykey__= primarykey
    return node

def hashtags_to_nodes(ents):
    out= []
    if ents['hashtags']:
        for each in ents['hashtags']:
            out.append(dict_to_node(each,'Hashtag',primarykey='text',))
    return out

def mentions_to_nodes(ents):
    out=[]
    if ents['user_mentions']:
        for each in ents['user_mentions']:
            each.pop('indices')
            out.append(user_dtn(each))
    return out

def urls_to_nodes(ents):
    out=[]
    if ents['urls']:
        for each in ents['urls']:
            each.pop('indices')
            out.append(dict_to_node(each,'Url',primarykey='expanded_url',primarylabel='Url'))
    return out

def media_to_nodes(ents):
    out= []
    if ents['media']:
        for each in ents['media']:
            each.pop('indices')
            out.append(dict_to_node(each,'Media',each['type'].title(),primarykey='id',primarylabel='Media'))
    return out

def ent_parser(ents):
    output={}
    dents = defaultdict(int)
    dents.update(ents)
    output['hashtags']= hashtags_to_nodes(dents)
    output['mentions']= mentions_to_nodes(dents)
    output['urls']= urls_to_nodes(dents)
    output['media']= media_to_nodes(dents)
    return {k:v for (k,v) in output.items() if v}

def user_dtn(datadict):
#     if datadict['id'] in user_ids:
#         return dict_to_node(datadict,'Target',primarykey='id',primarylabel='User',)
    return dict_to_node(datadict,'User',primarykey='id',primarylabel='User')



def seperate_children(tweet):
    try:
        retweeted = tweet.pop('retweeted_status')
    except:
        retweeted = []
    try:
        quoted = tweet.pop('quoted_status')
    except:
        quoted = []

    output=defaultdict(int)
    try:
        output['user'] = tweet.pop('user')
    except:
        output['user']=[]
    try:
        output['ents'] = tweet.pop('entities')
    except:
        output['ents'] = []
    output['tweet'] = dict(tweet)

    if isinstance(retweeted,dict) and isinstance(quoted,dict):
        retweeted.pop('quoted_status')
        output['qtuser'] = quoted.pop('user')
        output['qents'] = quoted.pop('entities')

        output['rtuser'] =retweeted.pop('user')
        output['rents']=retweeted.pop('entities')
        output['retweeted'] = retweeted

        output['quoted'] = quoted

    elif isinstance(quoted,dict):
        output['qtuser'] = quoted.pop('user')
        output['qents'] = quoted.pop('entities')
        output['quoted'] = quoted


    elif isinstance(retweeted,dict):
        output['rtuser']= retweeted.pop('user')
        output['rents']= retweeted.pop('entities')
        output['retweeted']=retweeted

    return output

def push_tweet(tweetdict):
    dicts=seperate_children(tweetdict)
    tx = graph.begin()
    if isinstance(dicts['user'],dict):
        user =  user_dtn(dicts['user'])
    else:
        gaffer = user_dtn(dicts['tweet']['delete']['status'])
        regret = dict_to_node(dicts['tweet']['delete']['status'],'Tweet')
        deleted = Relationship(gaffer,'DELETES',regret,timestamp=dicts['tweet']['delete']['timestamp_ms'])
        tx.merge(gaffer,primary_key='id')
        tx.merge(regret,primary_key='id')
        tx.merge(deleted)
        tx.commit()
        return
        
    tweet = dict_to_node(dicts['tweet'],'Tweet')

    tx.merge(user,primary_key='id')


    if 'retweeted' in dicts.keys() and 'quoted' in dicts.keys():
        tweet.add_label('Retweet')
        retweet = dict_to_node(dicts['retweeted'],'Tweet','Qtweet')
        quoted = dict_to_node(dicts['quoted'], 'Tweet')
        rtuser = user_dtn(dicts['rtuser'])
        qtuser = user_dtn(dicts['qtuser'])

        tweeted = Relationship(user,'TWEETS', tweet, timestamp= tweet['timestamp'],
                               created_at = tweet['created_at'], usrStatusCount= user['statuses_count'],
                              usrFollowerCount= user['followers_count'], usrFavoritesCount = user['favourites_count'])

        tweeted2 = Relationship(rtuser,'TWEETS', retweet,timestamp= retweet['timestamp'],
                                created_at = retweet['created_at'], usrStatusCount= rtuser['statuses_count'],
                              usrFollowerCount= rtuser['followers_count'], usrFavoritesCount = rtuser['favourites_count'])

        tweeted3 = Relationship(qtuser,'TWEETS',quoted, timestamp= quoted['timestamp'],
                                created_at = quoted['created_at'], usrStatusCount= qtuser['statuses_count'],
                              usrFollowerCount= qtuser['followers_count'], usrFavoritesCount = qtuser['favourites_count'])

        retweeted = Relationship(tweet,'RETWEETS',retweet, timestamp= tweet['timestamp'], favcount=retweet['favorite_count'],
                                 createdAt=tweet['created_at'],
                                replyCount= retweet['reply_count'], sourceFollowers = rtuser['followers_count'],
                                retweetCount=retweet['retweet_count'],quoteCount=retweet['quote_count'])

        quotes = Relationship(retweet,'QUOTES',quoted,timestamp= quoted['timestamp'],
                                favcount=quoted['favourites_count'],
                                replyCount= quoted['reply_count'], sourceFollowers = qtuser['followers_count'], createdAt= retweet['created_at'],
                                retweetCount=quoted['retweet_count'],quoteCount=quoted['quote_count'])

        tx.merge(tweet,primary_key='id')
        tx.merge(user,primary_key='id')
        tx.merge(tweeted)


        tx.merge(rtuser,primary_key = 'id')
        tx.merge(retweet,primary_key='id')
        tx.merge(tweeted2)

        tx.merge(qtuser,primary_key='id')
        tx.merge(quoted,primary_key='id')
        tx.merge(retweeted)
        tx.merge(tweeted3)
        tx.merge(quotes)

#         for ent,ls in ent_parser(dicts['rents']).items():
#             for each in ls:
#                 contains= Relationship(retweet,'CONTAINS',each)
#                 tx.merge(each,ent,primary_key=each.__primarykey__)
#                 tx.merge(contains)

        for ent,ls in ent_parser(dicts['qents']).items():
            if ls:
                for each in ls:

                    contains= Relationship(quoted,'CONTAINS',each)
                    tx.merge(each,ent, primary_key=each.__primarykey__)
                    tx.merge(contains)

        for ent,ls in ent_parser(dicts['rents']).items():
            if ls:
                for each in ls:
                    contains= Relationship(retweet,'CONTAINS',each)
                    tx.merge(each,ent,primary_key=each.__primarykey__)
                    tx.merge(contains)



    elif 'retweeted' in dicts.keys():
        tweet.add_label('Retweet')
        rtuser = user_dtn(dicts['rtuser'])
        retweet = dict_to_node(dicts['retweeted'],'Tweet')


        tweeted = Relationship(user,'TWEETS',tweet, timestamp= tweet['timestamp'],
                               created_at = tweet['created_at'], usrStatusCount= user['statuses_count'],
                              usrFollowerCount= user['followers_count'], usrFavoritesCount = user['favourites_count'])

        tweeted2 = Relationship(rtuser,'TWEETS',retweet,timestamp= retweet['timestamp'],
                                created_at = retweet['created_at'], usrStatusCount= rtuser['statuses_count'],
                              usrFollowerCount= rtuser['followers_count'], usrFavoritesCount = rtuser['favourites_count'])
#         retweeted = Relationship(tweet,'RETWEETS',retweet,timestamp= retweet['timestamp'],
#                                 created_at = retweet['created_at'])
        retweeted = Relationship(tweet,'RETWEETS',retweet, timestamp= tweet['timestamp'], favcount=retweet['favourites_count'],
                                replyCount= retweet['reply_count'], sourceFollowers = rtuser['followers_count'], createdAt= tweet['created_at'],
                                retweetCount=retweet['retweet_count'],quoteCount=retweet['quote_count'])

        tx.merge(user,primary_key='id')
        tx.merge(tweet,primary_key='id')
        tx.merge(tweeted)
        tx.merge(rtuser,primary_key = 'id')
        tx.merge(retweet,primary_key='id')
        tx.merge(tweeted2)
        tx.merge(retweeted)
        for ent,ls in ent_parser(dicts['rents']).items():
            if ls:
                for each in ls:
                    contains= Relationship(retweet,'CONTAINS',each)
                    tx.merge(each,ent,primary_key=each.__primarykey__)
                    tx.merge(contains)



    elif 'quoted' in dicts.keys():
        tweet.add_label('Qtweet')
        qtuser = user_dtn(dicts['qtuser'])
        quoted = dict_to_node(dicts['quoted'],'Tweet')

        tweeted = Relationship(user,'TWEETS',tweet, timestamp= tweet['timestamp'],
                               created_at = tweet['created_at'], usrStatusCount= user['statuses_count'],
                              usrFollowerCount= user['followers_count'], usrFavoritesCount = user['favourites_count'])

        tweeted2 = Relationship(qtuser,'TWEETS',quoted,timestamp= quoted['timestamp'],
                                created_at = quoted['created_at'], usrStatusCount= qtuser['statuses_count'],
                              usrFollowerCount= qtuser['followers_count'], usrFavoritesCount = qtuser['favourites_count'])

        quotes = Relationship(tweet,'QUOTES',quoted, timestamp= tweet['timestamp'], favcount=quoted['favourites_count'],
                                replyCount= quoted['reply_count'], sourceFollowers = qtuser['followers_count'], createdAt= tweet['created_at'],
                                retweetCount=quoted['retweet_count'],quoteCount=quoted['quote_count'])

        tx.merge(tweet,primary_key='id')
        tx.merge(user,primary_key='id')
        tx.merge(tweeted)
        tx.merge(qtuser,primary_key='id')
        tx.merge(quoted,primary_key='id')
        tx.merge(tweeted2)
        tx.merge(quotes)

        for ent,ls in ent_parser(dicts['ents']).items():
            if ls:
                for each in ls:
                    contains= Relationship(tweet,'CONTAINS',each)
                    tx.merge(each,ent,primary_key=each.__primarykey__)
                    tx.merge(contains)

        for ent,ls in ent_parser(dicts['qents']).items():
            if ls:
                for each in ls:
                    contains= Relationship(quoted,'CONTAINS',each)
                    tx.merge(each,ent,primary_key=each.__primarykey__)
                    tx.merge(contains)


#     subg = tweeted

    else:
        tweeted = Relationship(user,'TWEETS',tweet, timestamp= tweet['timestamp'],
                               created_at = tweet['created_at'], usrStatusCount= user['statuses_count'],
                              usrFollowerCount= user['followers_count'], usrFavoritesCount = user['favourites_count'])
        tx.merge(tweet,primary_key='id')
        tx.merge(user,primary_key='id')
        tx.merge(tweeted)
        for ent,ls in ent_parser(dicts['ents']).items():
            if ls:
                for each in ls:
                    contains= Relationship(tweet,'CONTAINS',each)
        #             subg = subg | contains
                    tx.merge(each,str(ent),primary_key=each.__primarykey__)
                    tx.merge(contains)

    tx.commit()
