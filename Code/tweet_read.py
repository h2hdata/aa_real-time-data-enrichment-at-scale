import os
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import socket
import json

# ACCESS_TOKEN = '506013389-ToSsEv0BZm80lZ9iJ5Ir58wRWsfDISybb1XCFSer'
# ACCESS_SECRET = '5CbGqJ9sUkRkZkFsEpetQicb5TTRkNkRyi82UTYdF2VHC'
# CONSUMER_KEY = 'segNwFJ36baq71sw4gODeBsYm'
# CONSUMER_SECRET = 'Y7N1Vhkbua5ojwIJLke0rusdZ7W3elqcSEWXKShp7rHlw1KTg5'
# my_auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET,ACCESS_TOKEN, ACCESS_SECRET)


consumer_key    = 'segNwFJ36baq71sw4gODeBsYm'#os.environ['TWITTER_CONSUMER_KEY']
consumer_secret = 'Y7N1Vhkbua5ojwIJLke0rusdZ7W3elqcSEWXKShp7rHlw1KTg5'#os.environ['TWITTER_CONSUMER_SECRET']
access_token    = '506013389-ToSsEv0BZm80lZ9iJ5Ir58wRWsfDISybb1XCFSer'#os.environ['TWITTER_ACCESS_TOKEN']
access_secret   = '5CbGqJ9sUkRkZkFsEpetQicb5TTRkNkRyi82UTYdF2VHC'#os.environ['TWITTER_ACCESS_SECRET']
 
class TweetsListener(StreamListener):
 
	def __init__(self, csocket):
		self.client_socket = csocket
 
	def on_data(self, data):
		try:
			print(data.split('\n'))
			self.client_socket.send(data)
			return True
		except BaseException as e:
			print("Error on_data: %s" % str(e))
		return True
 
	def on_error(self, status):
		print(status)
		return True
 
def sendData(c_socket):
	auth = OAuthHandler(consumer_key, consumer_secret)
	auth.set_access_token(access_token, access_secret)
 
	twitter_stream = Stream(auth, TweetsListener(c_socket))
	twitter_stream.filter(track=['weekend'])
 
if __name__ == "__main__":
	s = socket.socket()     # Create a socket object
	host = "localhost"      # Get local machine name
	port = 5556             # Reserve a port for your service.
	s.bind((host, port))    # Bind to the port
 
	print("Listening on port: %s" % str(port))
 
	s.listen(5)                 # Now wait for client connection.
	c, addr = s.accept()        # Establish connection with client.
 
	print( "Received request from: " + str( addr ) )
 
	sendData( c )
