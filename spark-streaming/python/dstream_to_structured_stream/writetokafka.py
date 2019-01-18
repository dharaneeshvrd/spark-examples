import sys
from kafka import KafkaProducer

if len(sys.argv) != 4:
    print "Usage Guide: python writetokafka.py <bootstrapServers> <topic> <fileToBeRead>"
    exit()

bootstrapServers = sys.argv[1]
topic = sys.argv[2]
fileToBeRead = sys.argv[3]

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

def main():
    kafka = streamToKafka()

    with open(fileToBeRead) as f:
        for row in f.readlines():
            row = row.strip('\n')
            kafka.publish_message(topic, row)

if __name__ == '__main__':
    main()
