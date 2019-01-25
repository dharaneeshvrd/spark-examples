import json
import time
import sys
from collections import OrderedDict
from kafka import KafkaProducer

from yahoo_weather import YahooWeather


class streamToKafka(object):

    def __init__(self, bootstrap_server, topic):
        self.topic = topic
        self.producer = None

        try:
            self.producer = KafkaProducer(bootstrap_servers=[bootstrap_server])
        except Exception as ex:
            print ex

    def publish_message(self, value):
        producer = self.producer.send(self.topic, value)

        while not producer.is_done:
            continue
        print value

def main():
    config = None

    with open("config.json") as f:
        config = json.load(f) 

    kafka = streamToKafka(config['bootstrap_server'], config['topic'])
    weather_client = YahooWeather(config['app_id'], config['consumer_key'], config['consumer_secret'], config['location'], config['unit'])

    while True:
        current = weather_client.get_current_weather()

        to_write = OrderedDict()
        to_write['location'] = config['location']
        to_write['temp_c'] = current['current_observation']['condition']['temperature']
        to_write['timestamp_s'] = str(int(time.time()))

        kafka.publish_message(json.dumps(to_write))
        time.sleep(60)

if __name__ == '__main__':
    main()
