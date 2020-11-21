import json
from tweepy import OAuthHandler,Stream, StreamListener
from datetime import datetime

# login with keys
consumer_key = ""
consumer_secret = ""

access_token = ""
access_token_secret = ""

#output file to store collected tweets
today = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
out = open(f"collected_tweets_{today}.txt","w")

#class to connect with Twitter
class MyListener(StreamListener):

    def on_data(self, data):
        #print(data)
        itemString = json.dumps(data)
        out.write(itemString + "\n")
        return True

    def on_error(self,status):
        print(status)

#main function
if __name__ == "__main__":
    l = MyListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token,access_token_secret)

    stream = Stream(auth,l)
    stream.filter(track=["Trump"])