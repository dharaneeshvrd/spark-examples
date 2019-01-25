import requests
import oauth2 as oauth
import time

class YahooWeather():
    def __init__(self, app_id, consumer_key, consumer_secret, location, unit):
        self.app_id = app_id
        self.consumer_key = consumer_key
        self.consumer_secret = consumer_secret
        self.location = location
        self.unit = unit

    def get_current_weather(self):
    
        url = "https://weather-ydn-yql.media.yahoo.com/forecastrss?location=%s&u=%s&format=json" % (self.location, self.unit)

        params = {
            'Yahoo-App-Id': self.app_id,
            'oauth_timestamp': str(int(time.time())),
            'oauth_signature_method': "HMAC-SHA1",
            'oauth_version': "1.0",
            'oauth_nonce': oauth.generate_nonce(),
            'oauth_consumer_key': self.consumer_key
        }

        req = oauth.Request(method="GET", url=url, parameters=params)

        consumer = oauth.Consumer(key=self.consumer_key, secret=self.consumer_secret)
        signature_method = oauth.SignatureMethod_HMAC_SHA1().sign(req, consumer, None)
        req['oauth_signature'] = signature_method

        weather_info = requests.get(req.to_url()).json()

        return weather_info
