import bson, datetime, ConfigParser, random
import logging, urllib2, sys
import pymongo, tweepy

CONFIG = sys.argv[1]
screen_name = sys.argv[2]
logging.basicConfig(format='%(asctime)s %(message)s', filename='run_log/auto_tweet.out',
  datefmt='%m/%d/%Y %H:%M:%S', level=logging.DEBUG)
logging.info('screen name is %s', screen_name)
logging.info('configuration file: %s', CONFIG)


OLD_DATE = datetime.datetime(1900,01,01)
E = .6

class mongo_config(object):
  def __init__(self, config):
    self._username = config.get('mongodb', 'username')
    self._password = config.get('mongodb', 'password')
    self._host = config.get('mongodb', 'host')
    self._port = config.get('mongodb', 'port')
    self.db = config.get('mongodb', 'db')
    self.collection = config.get(screen_name, 'mongodb_collection')
    self.url = 'mongodb://{user}:{password}@{host}:{port}/{database}'
    self.url = self.url.format(user=self._username, password=self._password,
      host=self._host, port = self._port, database=self.db )

class twitter_config(object):
  def __init__(self, screen_name, config):
    self._consumer_key = config.get('twitter_oauth', 'consumer_key')
    self._consumer_secret = config.get('twitter_oauth', 'consumer_secret')
    self._access_key = config.get(screen_name, 'access_key')
    self._access_secret = config.get(screen_name, 'access_secret')
    self._auth = tweepy.OAuthHandler(self._consumer_key, self._consumer_secret)
    self._auth.set_access_token(self._access_key, self._access_secret)
    self.api = tweepy.API(self._auth)

def read_file(data_file):
  data = urllib2.urlopen(data_file).readlines()
  return data

config = ConfigParser.ConfigParser()
config.readfp(open(CONFIG))
data_file = config.get(screen_name, 'datafile_url')
mc = mongo_config(config)

conn = pymongo.Connection(mc.url)

# learn how to make this dynamic
logging.info('mongodb database is %s', mc.db)

db = conn[mc.db]

logging.info("mongodb collection is %s", mc.collection)
coll=db[mc.collection]



# read the file, in this case its from a dropbox URL
itms = read_file(data_file)
items_dict = {}

# check each item to see if its in the db. if not, add it
# initialize with date 1900-01-01.
# note, I need to find a better way to do this

for itm in itms:
  # as we iterate through the items, add to a dict.
  # I was going to do something cute like use dict(zip(itms,[1 for x in items]))
  # but this is just as easy since we're already iteraterating through 
  # the items
  items_dict[itm] = 1
  rec=coll.find({'text':itm})
  if rec.count() == 0:
    logging.info("adding new item to database: %s", itm.strip())
    coll.insert({'text':itm,'count':0,'last_posted': OLD_DATE})

# delete any documents not found in the text file
# we do this by iterating through the collection(database)
# and then performing a lookup in items_dict for each.
# if we don't find a match, then the txt was removed from the file...
# so remove it from mongo.
# note: i wonder if we should just mark it as inactive instead of deleting
for x in coll.find():
  if not items_dict.get(x['text']):
    logging.info("deleting text %s", x['text'])
    coll.remove({'_id':x['_id']})

# end of file-database synchronization.
# now, based on the number of documents we have in our db (cnt)
# we'll find something at random which hasn't posted in at least
# E*len(itms) days (per tim).

last_date = datetime.datetime.now() - datetime.timedelta(int(E * len(itms)))

recs = coll.find({'last_posted':{'$lte': last_date}},{'text':1})

# save the list in case we need to re-use it
recs_list = [x for x in recs] 
next_tweet = random.choice(recs_list)
logging.info ("next tweet: %s-%s", next_tweet['_id'],next_tweet['text'].strip())

tc = twitter_config(screen_name, config)
tc.api.update_status(next_tweet['text'].strip())

# now that we've fired the tweet, lets increment the count and the updated date
coll.update({'_id':next_tweet['_id']},{'$inc':{'count':1},
  '$set':{'last_posted':datetime.datetime.now()}})
