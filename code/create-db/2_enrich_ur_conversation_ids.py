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
def enrich_ur_conversation_ids(password: str):
    """Enrich tweet database with ur-conversation ids"""
    with closing(mariadb.connect(user="convoy",
                                 password=password,
                                 host="vm1788.kaj.pouta.csc.fi",
                                 port=3306,
                                 database="convoy",
                                 autocommit=True)) as conn, closing(conn.cursor()) as cur:
        conn: MySQLConnection
        cur: MySQLCursor
        logging.info("Creating conversation ID map.")
        cur.execute("DROP TABLE IF EXISTS conversation_id_map_i;")
        cur.execute("""
                    CREATE TABLE conversation_id_map_i 
                    (PRIMARY KEY (from_conversation_id)) ENGINE=ARIA TRANSACTIONAL=0 PAGE_CHECKSUM=0
                    SELECT t1.conversation_id AS to_conversation_id, t2.conversation_id AS from_conversation_id FROM tweets_i t1 INNER JOIN tweets_i t2 ON t1.tweet_id = t2.quotes WHERE ISNULL(t2.in_reply_to)
                    UNION
                    SELECT t1.conversation_id AS to_conversation_id, t2.conversation_id AS from_conversation_id FROM tweets_i t1 INNER JOIN tweets_i t2 ON t1.tweet_id = t2.retweet_of
    
        """)
        logging.info("Walking conversation ID map to its roots.")
        while True:
            cur.execute("""
                UPDATE conversation_id_map_i cm1, conversation_id_map_i cm2
                SET cm2.to_conversation_id = cm1.to_conversation_id 
                WHERE cm1.from_conversation_id = cm2.to_conversation_id 
            """)
            logging.info("Iteration changed %s values.", cur.rowcount)
            if cur.rowcount == 0:
                break
        logging.info("Projecting ur-conversation ids to tweets table.")
        cur.execute("""
                    UPDATE tweets_i t LEFT JOIN conversation_id_map_i cim ON cim.from_conversation_id = t.conversation_id
                    SET ur_conversation_id = COALESCE(cim.to_conversation_id,t.conversation_id)
                    """)
        logging.info("Dropping conversation id map table.")
        cur.execute("DROP TABLE conversation_id_map_i")
        logging.info("Done.")


if __name__ == '__main__':
    enrich_ur_conversation_ids()
