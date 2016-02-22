import time
import redis


class Publisher():
    def __init__(self):
        self.redis_client = redis.Redis(host='localhost', port=6379, db=0)

    def publish_msg(self, channel, msg):
        self.redis_client.publish(channel, msg)




publisher = Publisher()
for i in range(1000):
    time.sleep(5)
    msg = '%d: hi how r u ?' % i
    print 'published message $ %s' % msg
    #if i%2 == 0:
    publisher.publish_msg('abcd1', msg)
    #else:
    #    publisher.publish_msg('abcd2', msg)
    #    publisher.publish_msg('abcd3', msg)
