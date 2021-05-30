import pandas as pd
import numpy as np
import yaml
import pymongo
import psycopg2
from sqlalchemy import create_engine
import re

def deEmojify(text):
    regrex_pattern = re.compile(pattern = "["
        u"\U0001F600-\U0001F64F"  # emoticons
        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        u"\U0001F680-\U0001F6FF"  # transport & map symbols
        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                           "]+", flags = re.UNICODE)
    return regrex_pattern.sub(r'',text)

# Read in mongo password
with open(r'/Users/samivanecky/Git/tweety-stream/yaml/mongo.yaml') as file:
    mongo_yaml = yaml.full_load(file)

# Read in AWS creds
with open(r'/Users/samivanecky/Git/tweeter/aws.yaml') as file:
    aws = yaml.full_load(file)

# Setup AWS RDS connection
conn = psycopg2.connect(
    host=aws['host'],
    password=aws['password'],
    user=aws['user'],
    port=aws['port'])

# Setup connection to tweeter DB
# Create engine string
connect_str = 'postgresql+psycopg2://' + aws['user'] + ':' + aws['password'] + '@' + aws['host'] + ':' + str(aws['port']) + '/'

# Create engine connection
engine = create_engine(connect_str)

c = engine.connect()
conn = c.connection

# Mongo keys & creds
mongo_pwd = mongo_yaml['password']

# Mongo connection
client = pymongo.MongoClient(f"mongodb+srv://sgi_mongo_usr:{mongo_pwd}@sgicluster.4ifog.mongodb.net/")
# Connect to database
db = client['nba']
# Connect to NBA Playoffs connection
playoffs = db['playoffs']

# Convert collection to pandas
tweets = pd.DataFrame(list(playoffs.find()))

# Add column for partitioning search word
tweets['search_word'] = 'NBA'

# Subset columns
tweets = tweets[['id', 'created_at', 'text', 'source', 'quote_count', 'reply_count', 'retweet_count', 'favorite_count', 'is_quote_status', 'search_word']]

# Reformat the text field to drop newlines, emojis, etc and clean up before uploading
tweets['text'] = tweets['text'].apply(deEmojify)

# Query table to get existing data
existing_tweets = pd.read_sql('SELECT id FROM stream_tweets', engine)

# Filter and create dataframe of only new tweets
new_tweets = tweets[~(tweets['id']).isin(existing_tweets['id'])]

# Upload new tweets to AWS RDS
new_tweets.to_sql('stream_tweets', engine, if_exists='append')