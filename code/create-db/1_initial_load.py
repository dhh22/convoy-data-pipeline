#!/usr/bin/env python3
import dataclasses
import itertools
import os
from dataclasses import dataclass, astuple, field
from functools import reduce
from typing import Iterable, TextIO

import click
from mysql.connector import MySQLConnection
from mysql.connector.cursor import MySQLCursor
from contextlib import closing
from more_itertools import chunked
from json import JSONDecodeError
import tqdm
import json
import logging
import mariadb

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')


class RecoveringCursor:
    def __init__(self, **config):
        self.config = config
        self.conn = mariadb.connect(**self.config)
        self.cur = self.conn.cursor()

    def executemany(self, stmt, data):
        if len(data) > 0:
            chunk_size = 1000
            for db in chunked(data, chunk_size):
                while True:
                    try:
                        for db2 in chunked(db, chunk_size):
                            self.cur.executemany(stmt, db2)
                            if self.cur.warnings > 0:
                                self.cur.execute("SHOW WARNINGS")
                                warnings = list(filter(lambda warning: warning[1] != 1062, self.cur.fetchall()))
                                if len(warnings) > 0:
                                    logging.warning(warnings)
                        break
                    except mariadb.InterfaceError:
                        logging.error(db)
                        self.cur.close()
                        self.conn.close()
                        self.conn = mariadb.connect(**self.config)
                        self.cur = self.conn.cursor()
                        chunk_size = max(chunk_size // 2, 1)
                    except mariadb.DataError:
                        logging.exception(data)
                        raise

    def fetchall(self):
        return self.cur.fetchall()

    def close(self):
        self.cur.close()
        self.conn.close()


@dataclass
class Tweet:
    ur_conversation_id: int | None
    conversation_id: int | None
    id: int | None
    author_id: int | None
    created_at: str | None
    retweet_count: int | None
    reply_count: int | None
    like_count: int | None
    quote_count: int | None
    lang: str | None
    text: str | None
    in_reply_to: int | None
    in_reply_to_user_id: int | None
    quotes: int | None
    retweet_of: int | None
    error_short: str | None
    error_detail: str | None
    original: bool
    hashtags: list[str] | None
    urls: list[str] | None
    mentions: list[int] | None

    @classmethod
    def map_tweet(cls, tweet: dict, original: bool, mentions_id_map: dict):
        in_reply_to = None
        in_reply_to_user_id = None
        quotes = None
        retweet_of = None
        text = tweet['text']
        hashtags = None
        urls = None
        mentions = None
        if 'entities' in tweet:
            if 'urls' in tweet['entities']:
                urls = list()
                urlmap = dict()
                for url in tweet['entities']['urls']:
                    if 'unwound_url' in url:
                        urlmap[url['url']] = url['unwound_url']
                        urls.append(url['unwound_url'])
                    elif 'expanded_url' in url:
                        urlmap[url['url']] = url['expanded_url']
                        urls.append(url['expanded_url'])
                    else:
                        urls.append(url['url'])
                for (ourl, nurl) in urlmap.items():
                    text = text.replace(ourl, nurl)
            if 'hashtags' in tweet['entities']:
                hashtags = list(map(lambda hashtag: hashtag['tag'],tweet['entities']['hashtags']))
            if 'mentions' in tweet['entities']:
                mentions = list()
                for mention in tweet['entities']['mentions']:
                    mentions_id_map[mention['username']] = int(mention['id'])
                    mentions.append(int(mention['id']))
        if 'referenced_tweets' in tweet:
            for ref_tweet in tweet['referenced_tweets']:
                if ref_tweet['type'] == 'retweeted':
                    retweet_of = int(ref_tweet['id'])
                elif ref_tweet['type'] == 'replied_to':
                    in_reply_to = int(ref_tweet['id'])
                    in_reply_to_user_id = int(tweet['in_reply_to_user_id'])
                else:
                    quotes = int(ref_tweet['id'])
        return cls(None,
                   int(tweet['conversation_id']),
                   int(tweet['id']),
                   int(tweet['author_id']),
                   tweet['created_at'][0:10] + ' ' + tweet['created_at'][11:18],
                   int(tweet['public_metrics']['retweet_count']),
                   int(tweet['public_metrics']['reply_count']),
                   int(tweet['public_metrics']['like_count']),
                   int(tweet['public_metrics']['quote_count']),
                   tweet['lang'],
                   text,
                   in_reply_to,
                   in_reply_to_user_id,
                   quotes,
                   retweet_of,
                   None,
                   None,
                   original,
                   hashtags,
                   urls,
                   mentions
                   )

    @classmethod
    def error(cls, error: dict, original: bool):
        return cls(None, None, int(error['resource_id']), None, None, None, None, None, None, None, None, None, None, None, None, error['title'], error['detail'], original, None, None, None)

    @staticmethod
    def prepare_tweets_tables(conn: MySQLConnection):
        with closing(conn.cursor()) as cur:
            cur.execute("DROP TABLE IF EXISTS tweets_i;")
            cur.execute("""
                CREATE TABLE tweets_i (
                ur_conversation_id BIGINT UNSIGNED,
                conversation_id BIGINT UNSIGNED, 
                tweet_id BIGINT UNSIGNED PRIMARY KEY,
                author_id BIGINT UNSIGNED,
                created_at DATETIME, 
                retweet_count INTEGER UNSIGNED, 
                reply_count INTEGER UNSIGNED, 
                like_count INTEGER UNSIGNED, 
                quote_count INTEGER UNSIGNED, 
                lang VARCHAR(3) CHARACTER SET utf8mb4,
                text VARCHAR(2000) CHARACTER SET utf8mb4,
                in_reply_to BIGINT UNSIGNED,
                in_reply_to_user_id BIGINT UNSIGNED,
                quotes BIGINT UNSIGNED,
                retweet_of BIGINT UNSIGNED,
                error VARCHAR(255) CHARACTER SET utf8mb4,
                error_detail VARCHAR(255) CHARACTER SET utf8mb4,
                original BOOLEAN,
                hashtags INTEGER UNSIGNED,
                urls INTEGER UNSIGNED,
                mentions INTEGER UNSIGNED,
                INDEX(ur_conversation_id, tweet_id),
                INDEX(conversation_id, tweet_id)
                ) ENGINE=ARIA TRANSACTIONAL=0 PAGE_CHECKSUM=0""")
            cur.execute("DROP TABLE IF EXISTS tweet_hashtags_a")
            cur.execute("""
                         CREATE TABLE tweet_hashtags_a (
                             tweet_id BIGINT UNSIGNED,
                             hashtag VARCHAR(255) CHARACTER SET utf8mb4,
                             PRIMARY KEY (hashtag, tweet_id),
                             INDEX (tweet_id, hashtag)
                         ) ENGINE=ARIA TRANSACTIONAL=0 PAGE_CHECKSUM=0
                         """)
            cur.execute("DROP TABLE IF EXISTS tweet_urls_a")
            cur.execute("""
                         CREATE TABLE tweet_urls_a (
                             tweet_id BIGINT UNSIGNED,
                             url VARCHAR(570) CHARACTER SET utf8mb4,
                             PRIMARY KEY (url, tweet_id),
                             INDEX (tweet_id, url)
                         ) ENGINE=ARIA TRANSACTIONAL=0 PAGE_CHECKSUM=0
                         """)
            cur.execute("DROP TABLE IF EXISTS tweet_mentions_a")
            cur.execute("""
                         CREATE TABLE tweet_mentions_a (
                             tweet_id BIGINT UNSIGNED,
                             user_id BIGINT UNSIGNED,
                             PRIMARY KEY (user_id, tweet_id),
                             INDEX (tweet_id, user_id)
                         ) ENGINE=ARIA TRANSACTIONAL=0 PAGE_CHECKSUM=0
                         """)

    def as_tuple(self):
        return tuple(getattr(self, field.name) if field.name != "hashtags" and field.name != "mentions" and field.name != "urls" or getattr(self, field.name) is None else len(getattr(self, field.name)) for field in dataclasses.fields(self))

    insert_stmt = f"INSERT IGNORE INTO tweets_i VALUES ({'%s,'*20}%s)"

    insert_hashtags_stmt = "INSERT IGNORE INTO tweet_hashtags_a VALUES (%s, %s)"

    insert_urls_stmt = "INSERT IGNORE INTO tweet_urls_a VALUES (%s, %s)"

    insert_mentions_stmt = "INSERT IGNORE INTO tweet_mentions_a VALUES (%s, %s)"


@dataclass
class User:
    id: int
    username: str | None
    name: str | None
    description: str | None
    created_at: str | None
    verified: bool | None
    protected: bool | None
    url: str | None
    location: str | None
    followers_count: int | None
    following_count: int | None
    tweet_count: int | None
    listed_count: int | None
    error_short: str | None
    error_detail: str | None

    @classmethod
    def map_user(cls, user: dict):
        url = user['url']
        description = user['description']
        if 'entities' in user:
            urlmap = dict()
            for field_entities in user['entities'].values():
                if 'urls' in field_entities:
                    for murl in field_entities['urls']:
                        if 'unwound_url' in murl:
                            urlmap[url['url']] = murl['unwound_url']
                        elif 'expanded_url' in murl:
                            urlmap[murl['url']] = murl['expanded_url']
            for (ourl, nurl) in urlmap.items():
                url = url.replace(ourl, nurl)
                description = description.replace(ourl, nurl)

        return cls(
            int(user['id']),
            user['username'],
            user['name'],
            description if description != '' else None,
            user['created_at'][0:10] + ' ' + user['created_at'][11:18],
            user['verified'],
            user['protected'],
            url if url != '' else None,
            user['location'] if 'location' in user and user['location'] != '' else None,
            user['public_metrics']['followers_count'],
            user['public_metrics']['following_count'],
            user['public_metrics']['tweet_count'],
            user['public_metrics']['listed_count'],
            None,
            None
        )

    @classmethod
    def error(cls, id: int, error: dict):
        return cls(id, None, None, None, None, None, None, None, None, None, None, None, None, error['title'], error['detail'])

    @staticmethod
    def prepare_users_table(conn: MySQLConnection):
        with closing(conn.cursor()) as cur:
            cur.execute("DROP TABLE IF EXISTS users_a;")
            cur.execute("""
                CREATE TABLE users_a (
                    user_id BIGINT UNSIGNED PRIMARY KEY,
                    username VARCHAR(20) CHARACTER SET utf8mb4,
                    name VARCHAR(1023) CHARACTER SET utf8mb4,
                    description VARCHAR(1023) CHARACTER SET utf8mb4,
                    created_at DATETIME,
                    verified BOOLEAN,
                    protected BOOLEAN,
                    url VARCHAR(1023) CHARACTER SET utf8mb4,
                    location VARCHAR(255) CHARACTER SET utf8mb4,
                    followers_count INTEGER UNSIGNED, 
                    following_count INTEGER UNSIGNED, 
                    tweet_count INTEGER UNSIGNED, 
                    listed_count INTEGER UNSIGNED,
                    error VARCHAR(255) CHARACTER SET utf8mb4,
                    error_detail VARCHAR(255) CHARACTER SET utf8mb4,
                    INDEX (created_at,user_id),
                    INDEX (verified,user_id),
                    INDEX (location,user_id),
                    INDEX (error,user_id)
                 ) ENGINE=ARIA TRANSACTIONAL=0 PAGE_CHECKSUM=0
                ;""")

    insert_stmt = f"INSERT IGNORE INTO users_a VALUES ({'%s,' * 14}%s)"


def yield_pages(tweet_file: TextIO, original: bool) -> Iterable[tuple[Iterable[Tweet],Iterable[User]]]:
    response = tweet_file.readline()
    line_number = 1
    while response:
        try:
            r = json.loads(response)
            mentions_id_map = dict()
            tweets: Iterable[Tweet] = map(lambda tweet: Tweet.map_tweet(tweet, original, mentions_id_map), r['data'])
            if 'tweets' in r['includes']:
                tweets = itertools.chain(tweets, map(lambda tweet: Tweet.map_tweet(tweet, original, mentions_id_map), r['includes']['tweets']))
            users = map(User.map_user, r['includes']['users'])
            if 'errors' in r:
                tweets = itertools.chain(tweets, map(lambda tweet: Tweet.error(tweet, original), filter(lambda e: e['resource_type'] == 'tweet', r['errors'])))
                users = itertools.chain(users, map(lambda error: User.error(int(error['resource_id']), error), filter(lambda e: e['parameter'] == 'in_reply_to_user_id', r['errors'])))
                users = itertools.chain(users, map(lambda error: User.error(mentions_id_map[error['resource_id']], error), filter(lambda e: e['parameter'] == 'entities.mentions.username', r['errors'])))
            yield tweets, users
        except JSONDecodeError:
            logging.exception(f"Exception parsing line number {line_number} of {tweet_file.name}. Skipping.")
        response = tweet_file.readline()
        line_number += 1


@click.option('-p', '--password', required=True, help="database password")
@click.command
def load_db(password: str):
    """Load tweets into the database"""
    with closing(mariadb.connect(user="convoy",
                                 password=password,
                                 host="vm1788.kaj.pouta.csc.fi",
                                 port=3306,
                                 autocommit=True)) as conn, closing(conn.cursor()) as cur:
        cur: MySQLCursor
        cur.execute("CREATE DATABASE IF NOT EXISTS convoy;")

    with closing(mariadb.connect(user="convoy",
                                 password=password,
                                 host="vm1788.kaj.pouta.csc.fi",
                                 port=3306,
                                 database="convoy",
                                 autocommit=True)) as conn:
        conn: MySQLConnection
        logging.info("Preparing tweets tables.")
        Tweet.prepare_tweets_tables(conn)
        logging.info("Preparing users table.")
        User.prepare_users_table(conn)
        logging.info("Loading data.")
        with closing(conn.cursor()) as cur:
            cur: MySQLCursor
            cur.execute("ALTER TABLE tweet_hashtags_a DISABLE KEYS;")
            cur.execute("ALTER TABLE tweet_mentions_a DISABLE KEYS;")
            cur.execute("ALTER TABLE tweet_urls_a DISABLE KEYS;")
            cur.execute("ALTER TABLE tweets_i DISABLE KEYS;")
            cur.execute("ALTER TABLE users_a DISABLE KEYS;")
        with closing(RecoveringCursor(user="convoy",
                                 password=password,
                                 host="vm1788.kaj.pouta.csc.fi",
                                 port=3306,
                                 database="convoy",
                                 autocommit=True)) as cur:
            cur: RecoveringCursor
            tweet_file_names = ["data/convoy_2weeks.jsonl", "data/convoy_conversations.jsonl",
                                "data/convoy_conversations_unprocessed.jsonl"]
            tsize = reduce(lambda tsize, tweet_file_name: tsize + os.path.getsize(tweet_file_name), tweet_file_names, 0)
            pbar = tqdm.tqdm(total=tsize, unit='b', unit_scale=True, unit_divisor=1024)
            processed_files_tsize = 0
            for tweet_file_name in tweet_file_names:
                original = tweet_file_name == "data/convoy_2weeks.jsonl"
                with open(tweet_file_name, "rt") as tweet_file:
                    for chunk_number, pages in enumerate(chunked(yield_pages(tweet_file, original), 10)):
                        tweets = list(itertools.chain.from_iterable(map(lambda page: page[0], pages)))
                        cur.executemany(Tweet.insert_stmt, [tweet.as_tuple() for tweet in tweets])
                        cur.executemany(Tweet.insert_hashtags_stmt, list(
                            itertools.chain.from_iterable(
                                map(lambda tweet: map(lambda hashtag: (tweet.id, hashtag), tweet.hashtags), filter(lambda tweet: tweet.hashtags is not None, tweets)))))
                        cur.executemany(Tweet.insert_mentions_stmt, list(
                            itertools.chain.from_iterable(
                                map(lambda tweet: map(lambda mention: (tweet.id, mention), tweet.mentions), filter(lambda tweet: tweet.mentions is not None, tweets)))))
                        cur.executemany(Tweet.insert_urls_stmt, list(
                            itertools.chain.from_iterable(
                                map(lambda tweet: map(lambda url: (tweet.id, url), tweet.urls), filter(lambda tweet: tweet.urls is not None, tweets)))))
                        cur.executemany(User.insert_stmt, list(map(lambda user: astuple(user), itertools.chain.from_iterable(map(lambda page: page[1], pages)))))
                        pbar.n = processed_files_tsize + tweet_file.tell()
                        pbar.update(0)
                processed_files_tsize += os.path.getsize(tweet_file_name)
        with closing(conn.cursor()) as cur:
            cur: MySQLCursor
            logging.info('Insert complete. Enabling keys.')
            cur.execute("ALTER TABLE tweet_hashtags_a ENABLE KEYS;")
            cur.execute("ALTER TABLE tweet_mentions_a ENABLE KEYS;")
            cur.execute("ALTER TABLE tweet_urls_a ENABLE KEYS;")
            cur.execute("ALTER TABLE tweets_i ENABLE KEYS;")
            cur.execute("ALTER TABLE users_a ENABLE KEYS;")
            logging.info('Done enabling keys.')


if __name__ == '__main__':
    load_db()
