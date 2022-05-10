#!/usr/bin/env python3
import logging
from contextlib import closing
import click
import mariadb
from mysql.connector.cursor import MySQLCursor

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')


@click.option('-p', '--password', required=True, help="database password")
@click.command
def create_tweets_a(password: str):
    """Create tweets_a table"""
    with closing(mariadb.connect(user="convoy",
                                 password=password,
                                 host="vm1788.kaj.pouta.csc.fi",
                                 port=3306,
                                 database="convoy",
                                 autocommit=True)) as conn, closing(conn.cursor()) as cur:
        cur: MySQLCursor
        logging.info("Creating tweets_a table.")
        cur.execute("DROP TABLE IF EXISTS tweets_a")
        cur.execute("""
            CREATE TABLE tweets_a (
                PRIMARY KEY (tweet_id),
                UNIQUE INDEX(ur_conversation_id, tweet_id),
                UNIQUE INDEX(conversation_id, tweet_id),
                UNIQUE INDEX(author_id, tweet_id),
                UNIQUE INDEX(created_at, tweet_id),
                UNIQUE INDEX(lang, tweet_id),
                FULLTEXT(text),
                UNIQUE INDEX(in_reply_to, tweet_id),
                UNIQUE INDEX(in_reply_to_user_id, tweet_id),
                UNIQUE INDEX(quotes, tweet_id),
                UNIQUE INDEX(retweet_of, tweet_id),
                UNIQUE INDEX(error, tweet_id),
                UNIQUE INDEX(original, tweet_id)            
            ) ENGINE=ARIA TRANSACTIONAL=0 PAGE_CHECKSUM=0
            SELECT *, DATE(created_at) AS date_created_at, YEAR(created_at) AS year_created_at, MONTH(created_at) AS month_created_at, DAY(created_at) AS day_created_at, WEEK(created_at, 1) AS week_created_at, HOUR(created_at) AS hour_created_at 
            FROM tweets_i t LEFT JOIN tweet_stats_i ts USING (tweet_id) 
            WHERE 0""")
        cur.execute("ALTER TABLE tweets_a DISABLE KEYS")
        cur.execute("INSERT INTO tweets_a SELECT *, DATE(created_at) AS date_created_at, YEAR(created_at) AS year_created_at, MONTH(created_at) AS month_created_at, DAY(created_at) AS day_created_at, WEEK(created_at, 1) AS week_created_at, HOUR(created_at) AS hour_created_at FROM tweets_i t LEFT JOIN tweet_stats_i ts USING (tweet_id)")
        logging.info('Insert complete. Enabling keys.')
        cur.execute("ALTER TABLE tweets_a ENABLE KEYS")
        logging.info("Done enabling keys.")


if __name__ == '__main__':
    create_tweets_a()


