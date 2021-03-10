from datetime import datetime
from typing import Any

from tweepy import Stream
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
import socket
import json

import settings


class TweetsListener(StreamListener):

    def __init__(self, application_socket: socket):
        super().__init__()
        self.client_socket = application_socket
        self.oauth_handler = OAuthHandler(settings.TWITTER['CONSUMER_KEY'], settings.TWITTER['CONSUMER_SECRET'])
        self.oauth_handler.set_access_token(settings.TWITTER['ACCESS_TOKEN'], settings.TWITTER['ACCESS_SECRET'])

    def on_data(self, data: Any):
        try:
            message = json.loads(data)
            # Tweets which have 'extended_tweet' attribute are long ones.
            if 'extended_tweet' in message:
                tweet = str(message['extended_tweet']['full_text'])
            else:
                tweet = str(message['text'])

            tweet = f"{tweet}{settings.ANALYSIS['TWEET_EOL']}".encode('utf-8').strip()

            self.client_socket.send(tweet)
            print(f"{datetime.now()} New tweet sent to Spark: {tweet}")

        except BaseException as e:
            print(f"Error sending tweet to spark: {str(e)}")

        return True

    def send_data(self, keyword):
        twitter_stream = Stream(self.oauth_handler, TweetsListener(self.client_socket))
        twitter_stream.filter(track=keyword, languages=["en"])


if __name__ == "__main__":
    socket = socket.socket()
    socket.bind((settings.SOCKET['HOST'], settings.SOCKET['PORT']))
    socket.listen()

    print(f"Socket listening in {settings.SOCKET['HOST']}:{settings.SOCKET['PORT']} and waiting for Spark")

    application_socket, address = socket.accept()

    twitter_listener = TweetsListener(application_socket)

    twitter_listener.send_data(keyword=['love'])
