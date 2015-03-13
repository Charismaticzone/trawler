#!/usr/bin/env python

"""
This script downloads all available Tweets for a given list of
usernames.

The script takes as input a text file which lists one Twitter username
per line of the file.  The script creates a [username].tweets file for
each username specified in the directory.

Your Twitter OAuth credentials should be stored in the file
twitter_oauth_settings.py.
"""

# Standard Library modules
import argparse
import codecs
import os
import sys
import time
import json

# Third party modules
from twython import Twython, TwythonError

# Local modules
from twitter_crawler import (CrawlTwitterTimelines, FindFriendFollowers, RateLimitedTwitterEndpoint,
                             get_console_info_logger, get_screen_names_from_file, 
                             save_screen_names_to_file, save_tweets_to_json_file)
# Kafka modules
from kafka.client import KafkaClient
from kafka.common import OffsetOutOfRangeError
from kafka.consumer import SimpleConsumer
from kafka.producer import SimpleProducer


# KAFKA_HOSTS = 'k01.istresearch.com:9092'
KAFKA_HOSTS = '192.168.77.27:9092'
KAFKA_INCOMING_TOPIC = 'twitter.incoming_usernames'
KAFKA_OUTGOING_TWEETS = 'twitter.crawled_tweets'
KAFKA_OUTGOING_FF = 'twitter.outgoing_usernames'
KAFKA_GROUP = 'twitter-kafka-monitor'



try:
    from twitter_oauth_settings import access_token, access_token_secret, consumer_key, consumer_secret
except ImportError:
    print "You must create a 'twitter_oauth_settings.py' file with your Twitter API credentials."
    print "Please copy over the sample configuration file:"
    print "  cp twitter_oauth_settings.sample.py twitter_oauth_settings.py"
    print "and add your API credentials to the file."
    sys.exit()

# Set up crawlers
ACCESS_TOKEN = Twython(consumer_key, consumer_secret, oauth_version=2).obtain_access_token()
twython = Twython(consumer_key, access_token=ACCESS_TOKEN)

def run_kafka_monitor(kafka_host=KAFKA_HOSTS):
    kafka_conn = KafkaClient(kafka_host)
    kafka_conn.ensure_topic_exists(KAFKA_INCOMING_TOPIC)
    consumer = SimpleConsumer(kafka_conn,
                              KAFKA_GROUP,
                              KAFKA_INCOMING_TOPIC,
                              auto_commit=True,
                              iter_timeout=1.0)
    while True:
        start = time.time()
        try:
            for message in consumer.get_messages():
                if message is None:
                    break
                try:
                    u = message.message.value
                except:
                    pass
                else:
                    send_kafka_tweets(u)
                    send_kafka_ff(u)
        except OffsetOutOfRangeError:
            consumer.seek(0,0)
        end = time.time()

def send_kafka_tweets(screen_name, kafka_host=KAFKA_HOSTS):
    # Crawl the tweets for the user
    sys.stdout = codecs.getwriter('utf8')(sys.stdout)
    logger = get_console_info_logger()

    crawler = CrawlTwitterTimelines(twython, logger)

    try:
        tweets = crawler.get_all_timeline_tweets_for_screen_name(screen_name)
        # print tweets
    except TwythonError as e:
        print "TwythonError: %s" % e
        if e.error_code == 404:
            logger.warn("HTTP 404 error - Most likely, Twitter user '%s' no longer exists" % screen_name)
        elif e.error_code == 401:
            logger.warn("HTTP 401 error - Most likely, Twitter user '%s' no longer publicly accessible" % screen_name)
        else:
            # Unhandled exception
            raise e
    else:
        # Send the tweets to kafka
        kafka_conn = KafkaClient(kafka_host)
        producer = SimpleProducer(kafka_conn)
        topic = KAFKA_OUTGOING_TWEETS
        kafka_conn.ensure_topic_exists(topic)
        print "Sending all tweets for user {0} to topic {1}".format(screen_name, topic)
        for tweet in tweets:
            try:
                req = json.dumps(tweet)
            except:
                print 'json dumps failed'
            else:
                response = producer.send_messages(topic, req)


def send_kafka_ff(screen_name, kafka_host=KAFKA_HOSTS):
    # Crawl the friends + followers for the user
    sys.stdout = codecs.getwriter('utf8')(sys.stdout)
    logger = get_console_info_logger()

    ff_finder = FindFriendFollowers(twython, logger)

    ff_screen_names = ff_finder.get_ff_screen_names_for_screen_name(screen_name)

    # Send the tweets to kafka
    kafka_conn = KafkaClient(kafka_host)
    producer = SimpleProducer(kafka_conn)
    topic = KAFKA_OUTGOING_FF
    kafka_conn.ensure_topic_exists(topic)
    print "Sending all friends + followers for user {0} to topic {1}".format(screen_name, topic)
    for name in ff_screen_names:
        try:
            req = json.dumps(name)
        except:
            print 'json dumps failed'
        else:
            response = producer.send_messages(topic, req)


def main():
    # Make stdout output UTF-8, preventing "'ascii' codec can't encode" errors

    # parser = argparse.ArgumentParser(description="")
    # parser.add_argument('screen_name_file')
    # args = parser.parse_args()

    run_kafka_monitor()

    # screen_names = get_screen_names_from_file(args.screen_name_file)


if __name__ == "__main__":
    main()
