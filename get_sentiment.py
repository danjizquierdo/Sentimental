import spacy
import twint
import tweepy
import json
# import config
import re

# auth = tweepy.OAuthHandler(config.consumer_key, config.consumer_secret)
# auth.set_access_token(config.access_token, config.access_token_secret)
# api = tweepy.API(auth)

# nlp = spacy.load("en_core_web_sm")

def listen(terms, amount):
    with open('tweets.json', 'w+') as t, open('users.json', 'w+') as u:
        for term in terms:
            for tweet in tweepy.Cursor(api.search,q=term, count=amount).items(amount):
                if (not tweet.retweeted) and ('RT @' not in tweet.text):
                    try:
                        tweet_={}
                        twitter_users.append(tweet.user.name)
                        tweet_time.append(tweet.created_at)
                        tweet_['created_at'] = tweet.created_at
                        tweet_['id'] = tweet.id
                        tweet_['text'] = tweet.text
                        if tweet.lang:
                            tweet_['lang'] = tweet.lang
                        if tweet.in_reply_to_status_id:
                            tweet_['in_reply_to_status_id'] = tweet.in_reply_to_status_id
                        if tweet.in_reply_to_user_id:
                            tweet_['in_reply_to_user_id'] = tweet.in_reply_to_user_id
                        tweet_['user_id'] = tweet.user.id
                        tweet_['coordinates'] = tweet.coordinates
                        hash_tag = re.search(r'\#\w*',tweet.text)
                        if hash_tag:
                            if isinstance(hash_tag,list):
                                # for tag in hash_tag: hash_tags.add(tag)
                                tweet_['hashtags']= hash_tag
                            else:
                                # hash_tags.add(hash_tag)
                                tweet_['hashtags']= [hash_tag]
                        json.dump(tweet_, t)
                    except Exception as e:
                        print(e)
                        print(term)

                    try:
                        user_={}
                        user_['id'] = tweet_['user_id']
                        user_['screen_name'] = tweet.user.screen_name
                        user_['followers_count'] = tweet.user.followers_count
                        user_['verified'] = tweet.user.verified
                        user_['created_at'] = tweet.user.created_at
                        if tweet.user.lang:
                            user_['lang'] = tweet.user.lang
                        json.dump(user_, u)
                    except Exception as e:
                        print(e)
                        print(term)

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
