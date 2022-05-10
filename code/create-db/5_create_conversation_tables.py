#!/usr/bin/env python3
import logging
from contextlib import closing

import click
import mariadb
from mysql.connector import MySQLConnection
from mysql.connector.cursor import MySQLCursor

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')


@click.option('-p', '--password', required=True, help="database password")
@click.command
def create_conversation_tables(password: str):
    """Create conversation tables"""
    with closing(mariadb.connect(user="convoy",
                                 password=password,
                                 host="vm1788.kaj.pouta.csc.fi",
                                 port=3306,
                                 database="convoy",
                                 autocommit=True)) as conn, closing(conn.cursor()) as cur:
        conn: MySQLConnection
        cur: MySQLCursor
        logging.info("Creating ur-conversation table.")
        cur.execute("DROP TABLE IF EXISTS ur_conversations_a")
        cur.execute("""
                    CREATE TABLE ur_conversations_a ENGINE=ARIA TRANSACTIONAL=0 PAGE_CHECKSUM=0
                    SELECT * FROM
                    (SELECT * FROM tweets_a t WHERE t.ur_conversation_id=t.tweet_id) AS a RIGHT JOIN
                    (SELECT t.ur_conversation_id, COUNT(*) AS uc_ur_descendants, COUNT(DISTINCT t.author_id) AS authors, SUM(t.reply_count) AS uc_ur_t_reply_count, SUM(t.like_count) AS uc_ur_t_like_count, SUM(t.quote_count) AS uc_ur_t_quote_count, SUM(t.retweet_count) AS uc_ur_t_retweet_count 
                     FROM tweets_a t
                     GROUP BY t.ur_conversation_id) AS b USING (ur_conversation_id)
                    """)
        logging.info("Creating conversation table.")
        cur.execute("DROP TABLE IF EXISTS conversations_a")
        cur.execute("""
                    CREATE TABLE conversations_a ENGINE=ARIA TRANSACTIONAL=0 PAGE_CHECKSUM=0
                    SELECT * FROM
                    (SELECT * FROM tweets_a t WHERE t.conversation_id=t.tweet_id) AS a RIGHT JOIN
                    (SELECT t.conversation_id, COUNT(*) AS c_descendants, COUNT(DISTINCT t.author_id) AS authors, SUM(t.reply_count) AS c_t_reply_count, SUM(t.like_count) AS c_t_like_count, SUM(t.quote_count) AS c_t_quote_count, SUM(t.retweet_count) AS c_t_retweet_count 
                     FROM tweets_a t
                     GROUP BY t.conversation_id) AS b USING (conversation_id)
                    """)
        logging.info("Done.")


if __name__ == '__main__':
    create_conversation_tables()

