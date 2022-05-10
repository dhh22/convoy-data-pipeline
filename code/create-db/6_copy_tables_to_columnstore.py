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


def copy_table(cur: MySQLCursor, tbl: str):
    logging.info(f"Copying {tbl} table to ColumnStore.")
    cur.execute(f"DROP TABLE IF EXISTS {tbl}_c")
    cur.execute(f"CREATE TABLE {tbl}_c ENGINE=ColumnStore SELECT * FROM {tbl}_a WHERE 0")
    cur.execute(f"INSERT INTO {tbl}_c SELECT * FROM {tbl}_a")


@click.option('-p', '--password', required=True, help="database password")
@click.command
def copy_to_columnstore(password: str):
    """Copy tables from Aria to ColumnStore"""
    with closing(mariadb.connect(user="convoy",
                                 password=password,
                                 host="vm1788.kaj.pouta.csc.fi",
                                 port=3306,
                                 database="convoy",
                                 autocommit=True)) as conn, closing(conn.cursor()) as cur:
        cur: MySQLCursor
        copy_table(cur, "tweets")
        copy_table(cur, "tweet_hashtags")
        copy_table(cur, "tweet_mentions")
        copy_table(cur, "tweet_urls")
        copy_table(cur, "users")
        copy_table(cur, "conversations")
        copy_table(cur, "ur_conversations")
        logging.info("Done.")


if __name__ == '__main__':
    copy_to_columnstore()
