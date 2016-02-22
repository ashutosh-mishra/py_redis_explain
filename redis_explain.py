import redis
import time


class RedisExplain():
    def __init__(self, channel):
        # Create a redis client
        self.redis_client = redis.Redis(host='localhost', port=6379, db=0)
        self.channel = channel
        self.pubsub = self.redis_client.pubsub()
        # Create a strict redis client
        # r = redis.StrictRedis(host='localhost', port=6379, db=0)

    def _get(self, key):
        return self.redis_client.get(key)

    def _set(self, key, value):
        self.redis_client.set(key, value)

    def _delete(self, key):
        self.redis_client.delete(key)

    def _sadd(self, key, members):
        self.redis_client.sadd(key, members)

    def _smembers(self, key):
        return self.redis_client.smembers(key)

    def _srem(self, key, member):
        self.redis_client.srem(key, member)

    def _create_pubsub(self, channel, handler=None):
        self.pubsub = self.redis_client.pubsub()
        # Channels/patterns can be subscribed as
        if handler:
            self.pubsub.subscribe(**{channel: handler})
        else:
            self.pubsub.subscribe(channel, 'channel-2')
            self.pubsub.subscribe('pattern-*')

    def _unsubscribe(self, channel):
        self.pubsub.unsubscribe(channel)

    def _get_messages_through_get(self):
        self._unsubscribe(self.channel)
        self._create_pubsub(self.channel)
        #while True:
        for i in range(20):
            msg = self.pubsub.get_message()
            if msg and msg['type'] == 'message':
                print '\nget_messages_through get = ', msg
            else:
                print '.',
                time.sleep(0.5)

    def _get_messages_through_listen(self):
        self._unsubscribe(self.channel)
        self._create_pubsub(self.channel)
        i = 0
        for msg in self.pubsub.listen():
            if msg['type'] == 'message':
                print '\nget_messages_through listen = ', msg
                i += 1
            if i == 5:
                break

    def _msg_handler(self, message):
        print 'MSG HANDLER: ', message['data']

    def _get_messages_through_thread(self):
        self._unsubscribe(self.channel)
        self._create_pubsub(self.channel, handler=self._msg_handler)
        thread = self.pubsub.run_in_thread(sleep_time=0.1)
        print 'Thread started - will stop thread after 15 secs'
        time.sleep(15)
        thread.stop()

    def get_published_messages(self):
        print "PubSub : Get published messages"
        # 3 ways to retrieve the messages :
        # 1) Through get_message()
        print 'getting messages through get'
        self._get_messages_through_get()

        # 2) Through listen(), which returns a generator
        print 'getting message through listen'
        self._get_messages_through_listen()

        # 3) Through running event loop in a seprate thread
        # prerequisite is subscribed channle should have register handler, else it'll
        # throw an exception and thread won't start.
        print 'getting messages in a seprate thread'
        self._get_messages_through_thread()


    def piping(self):
        print 'Piping through redis'
        # Pipeline - buffers set of commands and execute at once
        # In addition to executing cmnds, buffered cmnds are executed atomically as a
        # group.
        pipe = self.redis_client.pipeline()
        pipe.get('a').get('b').set('b', 'b-value').get('b')     # set of any redis cmnds
        pipe.execute()          # Returns a list of responses corresponding to each cmnd

        # All cmnds buffered into the pipeline, returns the pipeline object itself, so
        # calls can be chained as
        pipe.get('a').get('b').set('b', 'b-value').get('b').execute()


    def basic_usage(self):
        print 'Redis Basic usage'
        print "redis module supports all redis functionality, " \
                        " here we'll be showing basic functionality\n"
        key = raw_input("Enter key: ")
        value = raw_input("Enter value: ")
        member1 = raw_input("Enter mem1: ")
        member2 = raw_input("Enter mem2: ")

        get_val = self._get(key)
        print 'get key %s, value: %s' % (key, get_val)
        print 'Adding key: %s, value: %s' % (key, value)
        self._set(key, value)
        get_val = self._get(key)
        print 'After adding - get key %s, value: %s' % (key, get_val)
        print 'Deleting key: %s' % key
        self._delete(key)
        get_val = self._get(key)
        print 'get key %s, value: %s' % (key, get_val)

        get_val = self._smembers(key)
        print '\nget set members with key: %s, value : %s' % (key, get_val)
        self._sadd(key, member1)
        print 'Adding key: %s, member: %s' % (key, member1)
        self._sadd(key, member2)
        print 'Adding key: %s, member: %s' % (key, member2)
        get_val = self._smembers(key)
        print 'After adding in set - get set members ' +\
                    'with key: %s, value : %s' % (key, get_val)

        print 'Removing member: %s from set with key: %s' % (member1, key)
        self._srem(key, member1)
        get_val = self._smembers(key)
        print 'get set members with key: %s, value : %s' % (key, get_val)


redis_explain = RedisExplain('abcd1')
#redis_explain.basic_usage()
redis_explain.piping()
redis_explain.get_published_messages()

