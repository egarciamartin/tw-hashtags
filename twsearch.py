from twython import Twython
from twython import TwythonStreamer
from twython import Twython, TwythonAuthError, TwythonError
from pymongo import MongoClient
import time
import datetime
import sys


# The idea is to get random tweets. X number of tweets. 
# Save 500k tweets in mongo. collection: Tweetsexperiment
# Second part: update the users info to see the change. 

#create a separate method to get the keys! so they are private. 
f = open('private_keys.txt', 'r')
keys = f.read().splitlines()

try:
	APP_KEY = keys[0]
	APP_SECRET = keys[1]
	OAUTH_TOKEN = keys[2]
	OAUTH_TOKEN_SECRET = keys[3]
except :
	print "Exception: File missing some keys"
	sys.exit(0)

TOT_HOURS = 1 # 1 day
MIN = 12
KW = ["#JeSuisCharlie", "#CharieHebdo"]

client = MongoClient()
db = client.twscriptsearch
lusers = []


def createConnection():

	twitter = Twython(APP_KEY, APP_SECRET,
				  OAUTH_TOKEN, OAUTH_TOKEN_SECRET)
	return twitter

def processUsers(data):
	user = data ['user']['screen_name']
	followers = data ['user']['followers_count']
	friends = data ['user']['friends_count']
	ntweets = data ['user']['statuses_count']
	hashtag = data ['entities']['hashtags']
	lang = data ['lang']
	mentions = data ['entities']['user_mentions']
	RT_count = data ['retweet_count']
	reply_user = data['in_reply_to_screen_name']
	# Text contains retweet (RT) ?
	text = data ['text']
	if "RT " in text:
		RT_bool = True
	else:
		RT_bool = False

	if ( (RT_count != 0) or (mentions != []) or (reply_user != None) or RT_bool):

		newdata =  {"screen_name" : user, "hashtag" : hashtag, "statuses_count" : ntweets, 
					"followers_count" : followers , "friends_count" : friends, "lang" : lang,
					"mentions" : mentions, "reply_user" : reply_user, "RT_count": RT_count}
		db.users.save(newdata)
		lusers.append(user)
		print (user)
	else:

		print ("Not saving user")	




class MyStreamer(TwythonStreamer):
	counter = 0
	def on_success(self, data):
		if(self.counter < 500):
			if 'text' in data:
				#print data ['user']['screen_name']
				print data['text'].encode('utf-8')
				db.tweets.save(data)
				processUsers(data)
				self.counter = self.counter+1
		else:
			self.disconnect()


	def on_error(self, status_code, data):
		print status_code
		self.disconnect()

# open connection with Twitter
twitter = createConnection()
stream = MyStreamer(APP_KEY, APP_SECRET,
					OAUTH_TOKEN, OAUTH_TOKEN_SECRET)

# Procedure for picking users for one week:

# IMPORTANT: This loop runs every hour (with the sleep in 12 min and 5 updates.)
# We need to set the total number of hours: e.g.:24  , 1 day. 
for hours in range(TOT_HOURS):
	# 3k users per stream
	# stream.statuses.sample()
	stream.statuses.filter(track=KW)
	




