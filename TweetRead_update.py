"""
Majority of this code from http://www.awesomestats.in/spark-twitter-stream/

Adjustments were made as necessary to variables, ports, and stream filtering


"""


import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import socket
import json
import os

from dotenv import load_dotenv
load_dotenv(dotenv_path='/root/hw9/.env')

consumer_key = os.environ['TWITTER_CONSUMER_KEY']
consumer_secret = os.environ['TWITTER_CONSUMER_SECRET']
access_token = os.environ['TWITTER_ACCESS_TOKEN']
access_secret = os.environ['TWITTER_ACCESS_SECRET']

class TweetsListener(StreamListener):

  def __init__(self, csocket):
      self.client_socket = csocket

  def on_data(self, data):
      try:
          msg = json.loads( data, encoding='utf-8' )
          if msg['lang'] == 'en':
              user = msg['user']['screen_name'].encode('utf-8')
              mentions = []
              for mention in msg['entities']['user_mentions']:
                  mentions.append(mention['screen_name'].encode('utf-8'))
       
              hashtags = []
              for hashtag in msg['entities']['hashtags']:
                  hashtags.append(hashtag['text'].encode('utf-8'))

              if len(mentions) < 1:
                  mentions = ""
              if len(hashtags) <1:
                  hashtags = ""
              final_string=""

              first_pass = True
              if len(hashtags)>0 and len(mentions)>0:
                  for hashtag in hashtags:
                      if first_pass:
                          final_string = "%s|%s|%s"%(user,hashtag,mentions)
                          first_pass = False
                      else:
                          final_string = final_string + "*new*" + "%s|%s|%s"%(user,hashtag,mentions) 
              elif len(hashtags)>0:
                  for hashtag in hashtags:
                      if first_pass:
                          final_string = "%s|%s|"%(user,hashtag)
                          first_pass = False
                      else:
                          final_string=final_string + "*new*" + "%s|%s|"%(user,hashtag)
              if len(final_string)>0:
                  final_string = final_string + "\n"
                  final_string = unicode(final_string,'utf-8') 
                  print(final_string)
                  self.client_socket.send(final_string)
              else:
                  pass
          else:
              pass
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
#    twitter_stream.sample(languages=['en'])
    twitter_stream.filter(locations=[-180,-90,180,90])

if __name__ == "__main__":
  s = socket.socket()         # Create a socket object
  host = "169.45.88.196"      # Get local machine name
  port = 5555                 # Reserve a port for your service.
  s.bind((host, port))        # Bind to the port

  print("Listening on port: %s" % str(port))

  s.listen(1)                 # Now wait for client connection.
  c, addr = s.accept()        # Establish connection with client.

  print( "Received request from: " + str( addr ) )

  sendData( c )

