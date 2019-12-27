import spacy
import tweepy
import config

auth = tweepy.OAuthHandler(config.consumer_key, config.consumer_secret)
auth.set_access_token(config.access_token, config.access_token_secret)
api = tweepy.API(auth)

nlp = spacy.load("en_core_web_sm")

if __name__ == "__main__":
