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

TOT_HOURS = 24 # 1 day
MIN = 12

client = MongoClient()
db = client.twscript
lusers = []


def createConnection():

	twitter = Twython(APP_KEY, APP_SECRET,
				  OAUTH_TOKEN, OAUTH_TOKEN_SECRET)
	return twitter

def saveUsers(data):
	user = data ['user']['screen_name']
	followers = data ['user']['followers_count']
	friends = data ['user']['friends_count']
	ntweets = data ['user']['statuses_count']
	hashtag = data ['entities']['hashtags']
	newdata = {"screen_name" : user, "hashtag" : hashtag, "statuses_count" : ntweets, "followers_count" : followers , "friends_count" : friends}
	db.users.save(newdata)
	lusers.append(user)
	print (user)	

#For the document with screen_name:user, set the new values. 
#If the field does not exist, $set will add the field with the specified value. 
#If you specify a dotted path for a non-existent field
def updateUsers(upn):
	
	laux = lusers
	updt_users = []
	while(len(laux) > 1):
		if(len(laux) >100):
			updt_users = twitter.lookup_user(screen_name = laux[0:99])
			laux = laux[99:]
		else:
			updt_users = twitter.lookup_user(screen_name = laux[0:len(laux)])
			laux = {}

		# Update the user in the DB. updt users is a list with users.
		# each element in the list is a user object. 
		# updt_users['']
		print (updt_users)
		for user in updt_users:
			newdata = ({ '$set': {
	       		"followers_count" + upn: user['followers_count'] ,
	        	"friends_count" + upn: user['friends_count'] ,
	        	"statuses_count" + upn: user['statuses_count']}})
			db.users.update({"screen_name": user['screen_name'] }, newdata)


	return updt_users


class MyStreamer(TwythonStreamer):
	counter = 0
	def on_success(self, data):
		if(self.counter < 5):
			if 'text' in data:
				#print data ['user']['screen_name']
				print data['text'].encode('utf-8')
				db.tweets.save(data)
				saveUsers(data)
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
	stream.statuses.sample()
	for i in range(5):
		print ("sleeping for 12 minutes, until: " + str(datetime.datetime.now() + datetime.timedelta(minutes= 12) ))
		time.sleep(MIN * 60)
		updateUsers(str(i+2))







