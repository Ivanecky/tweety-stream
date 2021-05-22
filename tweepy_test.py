from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaProducer
import tweepy
import yaml
import pymongo
import json

with open(r'/Users/samivanecky/Git/tweety-stream/yaml/api.yaml') as file:
    api_yaml = yaml.full_load(file)

with open(r'/Users/samivanecky/Git/tweety-stream/yaml/mongo.yaml') as file:
    mongo_yaml = yaml.full_load(file)

# Keys & creds for Twitter API
api_key = api_yaml['api_key']
api_secret = api_yaml['api_secret']
bearer_token = api_yaml['bearer_token']
access_key = api_yaml['access_key']
access_token = api_yaml['access_token']

# Mongo keys & creds
mongo_pwd = mongo_yaml['password']

# Mongo connection
client = pymongo.MongoClient(f"mongodb+srv://sgi_mongo_usr:{mongo_pwd}@sgicluster.4ifog.mongodb.net/")
# Create database
db = client['nba']
# Create collection for the game 
poffD1 = db['poffD1']

def main():
    class StdOutListener(StreamListener):
        def on_data(self, data):
            db.gswMem.insert_one(json.loads(data))
            print(json.loads(data))
            return True
        def on_error(self, status):
            print (status)

    l = StdOutListener()
    auth = OAuthHandler(api_key, api_secret)
    auth.set_access_token(access_key, access_token)
    stream = Stream(auth, l)
    stream.filter(track="NBA")

if __name__ == "__main__":
    main() 

