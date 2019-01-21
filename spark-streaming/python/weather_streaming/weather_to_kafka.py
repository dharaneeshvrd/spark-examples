import json
import time
import sys
from collections import OrderedDict
from kafka import KafkaProducer

from apixu.client import ApixuClient, ApixuException

if len(sys.argv) != 5:
    print "Usage Guide: python writetokafka.py <bootstrapServers> <topic> <place> <api_key>"
    exit()

bootstrapServers = sys.argv[1]
topic = sys.argv[2]
place = sys.argv[3]
api_key = sys.argv[4]

class streamToKafka(object):

    def __init__(self):
        self.producer = None

        try:
            self.producer = KafkaProducer(bootstrap_servers=[bootstrapServers])
        except Exception as ex:
            print ex

    def publish_message(self, topic, value):
        producer = self.producer.send(topic, value)

        while not producer.is_done:
            continue
        print value

def main():
    kafka = streamToKafka()

    client = ApixuClient(api_key)

    while True:
        try:
            current = client.getCurrentWeather(q=place)

            to_write = OrderedDict()
            to_write['location'] = current['location']['name']
            to_write['temp_c'] = current['current']['temp_c']
            to_write['timestamp_s'] = current['location']['localtime_epoch']

            kafka.publish_message(topic, json.dumps(to_write))
        except ApixuException as e:
            print "ERROR: ", e

        time.sleep(60)

if __name__ == '__main__':
    main()
