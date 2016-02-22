import redis
import time


class Subscriber():
    def __init__(self):
        self.redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)
        self.pubsub = self.redis_client.pubsub()

    def subscriber_channels(self, channels):
        for channel in channels:
            self.pubsub.subscribe(channel)
            print 'subscribed channel: %s' % channel

        print '\n\n'
    def start_listening(self, channel):
        try:
            while True:
                msg = self.pubsub.get_message()
                if msg:
                    print msg['channel'], ' : ', msg['data']
                else:
                    time.sleep(2)
        except StopIteration:
            pass

s = Subscriber()

s.subscriber_channels(['abcd1', 'abcd2', 'abcd4'])

s.start_listening('abcd')
s.subscriber_channel('abcd3')

