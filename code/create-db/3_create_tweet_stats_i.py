#!/usr/bin/env python3
import logging
from contextlib import closing
from dataclasses import dataclass, field
from functools import reduce, lru_cache
from itertools import chain

import click
import mariadb
from more_itertools import chunked
from mysql.connector.cursor import MySQLCursor
from tqdm import tqdm

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')


@dataclass(eq=False)
class Tree:
    id: str
    author_id: int = -1
    reply_count: int = 0
    quote_count: int = 0
    like_count: int = 0
    retweet_count: int = 0
    
    children: set['Tree'] = field(default_factory=set, init=False)
    ur_children: set['Tree'] = field(default_factory=set, init=False)
    descendants: int = -1
    ur_descendants: int = -1
    leaf_descendants: int = -1
    ur_leaf_descendants: int = -1
    max_depth: int = -1
    ur_max_depth: int = -1
    sum_depth: int = -1
    ur_sum_depth: int = -1
    
    t_reply_count: int = -1
    ur_t_reply_count: int = -1
    t_retweet_count: int = -1
    ur_t_retweet_count: int = -1
    t_quote_count: int = -1
    ur_t_quote_count: int = -1
    t_like_count: int = -1
    ur_t_like_count: int = -1
    t_authors: set[int] = field(default_factory=set, init=False)
    ur_t_authors: set[int] = field(default_factory=set, init=False)

    mad_reply_count: float = -1
    ur_mad_reply_count: float = -1
    mad_quote_count: float = -1
    ur_mad_quote_count: float = -1
    mad_like_count: float = -1
    ur_mad_like_count: float = -1
    mad_retweet_count: float = -1
    ur_mad_retweet_count: float = -1
    mad_depth: float = -1
    ur_mad_depth: float = -1
    mad_t_authors: float = -1
    ur_mad_t_authors: float = -1

    def count_statistics(self):
        if self.descendants == -1:
            self.descendants = len(self.children)
            self.t_authors.add(self.author_id)
            self.ur_t_authors.add(self.author_id)
            self.ur_descendants = self.descendants + len(self.ur_children)
            self.leaf_descendants = reduce(lambda ld, node: ld+1 if len(node.children) == 0 else ld, self.children, 0)
            self.ur_leaf_descendants = self.leaf_descendants + reduce(lambda ld, node: ld+1 if len(node.children) == 0 and len(node.ur_children) == 0 else ld, self.ur_children, 0)
            self.max_depth = 0
            self.ur_max_depth = 0
            self.sum_depth = 0
            self.ur_sum_depth = 0
            
            self.t_reply_count = self.reply_count
            self.ur_t_reply_count = self.reply_count
            self.t_quote_count = self.quote_count
            self.ur_t_quote_count = self.quote_count
            self.t_retweet_count = self.retweet_count
            self.ur_t_retweet_count = self.retweet_count
            self.t_like_count = self.like_count
            self.ur_t_like_count = self.like_count
            
            stack: list[tuple[Tree, int, bool]] = list([(child, 1, True) for child in self.children])
            stack.extend(list([(child, 1, False) for child in self.ur_children]))
            while len(stack) > 0:
                (cur_node, cur_depth, cur_reply) = stack.pop()
                if cur_node.descendants != -1:
                    self.ur_t_authors.update(cur_node.ur_t_authors)
                    self.ur_descendants += cur_node.ur_descendants
                    self.ur_t_reply_count += cur_node.ur_t_reply_count
                    self.ur_t_quote_count += cur_node.ur_t_quote_count
                    self.ur_t_retweet_count += cur_node.ur_t_retweet_count
                    self.ur_t_like_count += cur_node.ur_t_like_count
                    if cur_node.ur_max_depth + cur_depth > self.ur_max_depth:
                        self.ur_max_depth = cur_node.ur_max_depth + cur_depth
                    self.ur_sum_depth += cur_node.ur_sum_depth + cur_depth * cur_node.ur_leaf_descendants
                    self.ur_leaf_descendants += cur_node.ur_leaf_descendants
                    if cur_reply:
                        self.t_authors.update(cur_node.t_authors)
                        self.descendants += cur_node.descendants
                        self.t_reply_count += cur_node.t_reply_count
                        self.t_quote_count += cur_node.t_quote_count
                        self.t_retweet_count += cur_node.t_retweet_count
                        self.t_like_count += cur_node.t_like_count
                        if cur_node.max_depth + cur_depth > self.max_depth:
                            self.max_depth = cur_node.max_depth + cur_depth
                        self.sum_depth += cur_node.sum_depth + cur_depth * cur_node.leaf_descendants
                        self.leaf_descendants += cur_node.leaf_descendants
                else:
                    self.ur_t_authors.add(cur_node.author_id)
                    self.ur_t_reply_count += cur_node.reply_count
                    self.ur_t_quote_count += cur_node.quote_count
                    self.ur_t_like_count += cur_node.like_count
                    self.ur_t_retweet_count += cur_node.retweet_count
                    cur_leaf_descendants = reduce(lambda ld, node: ld + 1 if len(node.children) == 0 else ld, cur_node.children, 0)
                    self.ur_leaf_descendants += cur_leaf_descendants + reduce(lambda ld, node: ld + 1 if len(node.children) == 0 else ld, cur_node.ur_children, 0)
                    self.ur_descendants += len(cur_node.children) + len(cur_node.ur_children)
                    if cur_reply:
                        self.t_authors.add(cur_node.author_id)
                        self.t_reply_count += cur_node.reply_count
                        self.t_quote_count += cur_node.quote_count
                        self.t_like_count += cur_node.like_count
                        self.t_retweet_count += cur_node.retweet_count
                        self.descendants += len(cur_node.children)
                        self.leaf_descendants += cur_leaf_descendants
                    if len(cur_node.children) == 0:
                        if cur_reply:
                            if cur_depth > self.max_depth:
                                self.max_depth = cur_depth
                            self.sum_depth += cur_depth
                        if len(cur_node.ur_children) == 0:
                            if cur_depth > self.ur_max_depth:
                                self.ur_max_depth = cur_depth
                            self.ur_sum_depth += cur_depth
                    stack.extend([(child, cur_depth + 1, cur_reply) for child in cur_node.children])
                    stack.extend([(child, cur_depth + 1, False) for child in cur_node.ur_children])
    
    def count_mads(self):
        mean_depth = self.sum_depth / self.leaf_descendants if self.leaf_descendants != 0 else 0.0
        self.mad_depth = reduce(lambda sum_mad_depth, child: sum_mad_depth + abs(mean_depth - child.max_depth), self.children, 0) / len(self.children) if self.leaf_descendants != 0 else 0.0
        ur_mean_depth = self.ur_sum_depth / self.ur_leaf_descendants if self.ur_leaf_descendants != 0 else 0.0
        self.ur_mad_depth = reduce(lambda sum_mad_depth, child: sum_mad_depth + abs(ur_mean_depth - child.max_depth), chain(self.children,self.ur_children), 0) / (len(self.children) + len(self.ur_children)) if self.ur_leaf_descendants != 0 else 0.0
        self.mad_t_authors = reduce(lambda sum_t_authors, child: sum_t_authors+len(child.t_authors), self.children, 0) / len(self.children) if self.descendants != 0 else 0.0
        self.ur_mad_t_authors = reduce(lambda sum_t_authors, child: sum_t_authors+len(child.ur_t_authors), chain(self.children, self.ur_children), 0) / (len(self.children) + len(self.ur_children)) if self.ur_leaf_descendants != 0 else 0.0

        mean_reply_count = self.t_reply_count / (self.descendants + 1)
        self.mad_reply_count = abs(self.reply_count - mean_reply_count)
        ur_mean_reply_count = self.ur_t_reply_count / (self.ur_descendants + 1)
        self.ur_mad_reply_count = abs(self.reply_count - ur_mean_reply_count)
        mean_quote_count = self.t_quote_count / (self.descendants + 1)
        self.mad_quote_count = abs(self.quote_count - mean_quote_count)
        ur_mean_quote_count = self.ur_t_quote_count / (self.ur_descendants + 1)
        self.ur_mad_quote_count = abs(self.quote_count - ur_mean_quote_count)
        mean_like_count = self.t_like_count / (self.descendants + 1)
        self.mad_like_count = abs(self.like_count - mean_like_count)
        ur_mean_like_count = self.ur_t_like_count / (self.ur_descendants + 1)
        self.ur_mad_like_count = abs(self.like_count - ur_mean_like_count)
        mean_retweet_count = self.t_retweet_count / (self.descendants + 1)
        self.mad_retweet_count = abs(self.retweet_count - mean_retweet_count)
        ur_mean_retweet_count = self.ur_t_retweet_count / (self.ur_descendants + 1)
        self.ur_mad_retweet_count = abs(self.retweet_count - ur_mean_retweet_count)

        stack: list[tuple[Tree, int, bool]] = list([(child, 1, True) for child in self.children])
        stack.extend(list([(child, 1, False) for child in self.ur_children]))
        while len(stack) > 0:
            (cur_node, cur_depth, cur_reply) = stack.pop()
            self.ur_mad_reply_count += abs(cur_node.reply_count - ur_mean_reply_count)
            self.ur_mad_quote_count += abs(cur_node.quote_count - ur_mean_quote_count)
            self.ur_mad_like_count += abs(cur_node.like_count - ur_mean_like_count)
            self.ur_mad_retweet_count += abs(cur_node.retweet_count - ur_mean_retweet_count)
            if cur_reply:
                self.mad_reply_count += abs(cur_node.reply_count - mean_reply_count)
                self.mad_quote_count += abs(cur_node.quote_count - mean_quote_count)
                self.mad_like_count += abs(cur_node.like_count - mean_like_count)
                self.mad_retweet_count += abs(cur_node.retweet_count - mean_retweet_count)

    def as_tuple(self):
        return (self.id,
                len(self.children),
                len(self.children) + len(self.ur_children),
                self.descendants,
                self.ur_descendants,
                self.leaf_descendants,
                self.ur_leaf_descendants,
                self.max_depth,
                self.ur_max_depth,
                len(self.t_authors),
                len(self.ur_t_authors),
                self.t_reply_count,
                self.ur_t_reply_count,
                self.t_quote_count,
                self.ur_t_quote_count,
                self.t_like_count,
                self.ur_t_like_count,
                self.t_retweet_count,
                self.ur_t_retweet_count,
                self.descendants / (1 + self.descendants - self.leaf_descendants),
                self.ur_descendants / (1 + self.ur_descendants - self.ur_leaf_descendants),
                self.sum_depth / self.leaf_descendants if self.leaf_descendants != 0 else 0.0,
                self.ur_sum_depth / self.ur_leaf_descendants if self.ur_leaf_descendants != 0 else 0.0,
                self.mad_depth,
                self.ur_mad_depth,
                self.t_reply_count / (1 + self.descendants),
                self.ur_t_reply_count / (1 + self.ur_descendants),
                self.mad_reply_count / (1 + self.descendants),
                self.ur_mad_reply_count / (1 + self.ur_descendants),
                self.t_quote_count / (1 + self.descendants),
                self.ur_t_quote_count / (1 + self.ur_descendants),
                self.mad_quote_count / (1 + self.descendants),
                self.ur_mad_quote_count / (1 + self.ur_descendants),
                self.t_like_count / (1 + self.descendants),
                self.ur_t_like_count / (1 + self.ur_descendants),
                self.mad_like_count / (1 + self.descendants),
                self.ur_mad_like_count / (1 + self.ur_descendants),
                self.t_retweet_count / (1 + self.descendants),
                self.ur_t_retweet_count / (1 + self.ur_descendants),
                self.mad_retweet_count / (1 + self.descendants),
                self.ur_mad_retweet_count / (1 + self.ur_descendants),
                )


def enrich_conversation(cur: MySQLCursor, tweets: list[tuple[int, int, int, int, int, int, int, int, int]]):
    tweet_trees = lru_cache(maxsize=None)(lambda id: Tree(id))
    for tweet in tqdm(tweets, unit="tweets", leave=False, desc="treeing"):
        tweet: tuple[int, int, int, int, int, int, int, int, int]
        tt = tweet_trees(tweet[0])
        tt.author_id = tweet[1]
        tt.reply_count = tweet[5]
        tt.quote_count = tweet[6]
        tt.like_count = tweet[7]
        tt.retweet_count = tweet[8]
        if tweet[2] is not None:
            tweet_trees(tweet[2]).children.add(tt)
        elif tweet[3] is not None:
            tweet_trees(tweet[3]).ur_children.add(tt)
        elif tweet[4] is not None:
            tweet_trees(tweet[4]).ur_children.add(tt)
    for tweet in tqdm(tweets, unit="tweets", leave=False, desc="descending"):
        tt = tweet_trees(tweet[0])
        tt.count_statistics()
        if tt.descendants > len(tweets):
            logging.error("Something is off. %s has %d descendants which is more than %d (tweets in ur-conversation).", tweet, tt.descendants, len(tweets))
        if tt.leaf_descendants > tt.descendants:
            logging.error("Something is off. %s has %d leaf descendants which is more than %d (descendants).", tweet, tt.leaf_descendants, tt.descendants)
        tt.count_mads()
    for tweet_batch in tqdm(list(chunked(tweets, 500)), unit="batches", leave=False, desc="inserting"):
        data = [tweet_trees(tweet[0]).as_tuple() for tweet in tweet_batch]
        try:
            cur.executemany(f"INSERT IGNORE INTO tweet_stats_i VALUES ({'%s,'*40}%s)", data)
        except mariadb.InterfaceError:
            logging.exception(f"InterfaceError for {data}")

int_cols = [
    'children',
    'descendants',
    'leaf_descendants',
    'max_depth',
    't_authors',
    't_reply_count',
    't_quote_count',
    't_like_count',
    't_retweet_count'
]

float_cols = [
    'branching_factor',
    'mean_depth',
    'depth_mad',
    'mean_reply_count',
    'reply_count_mad',
    'mean_quote_count',
    'quote_count_mad',
    'mean_like_count',
    'like_count_mad',
    'mean_retweet_count',
    'retweet_count_mad'
]


@click.option('-p', '--password', required=True, help="database password")
@click.command
def enrich_conversations(password: str):
    """Enrich conversations with statistical information"""
    with closing(mariadb.connect(user="convoy",
                                 password=password,
                                 host="vm1788.kaj.pouta.csc.fi",
                                 port=3306,
                                 database="convoy",
                                 autocommit=True)) as conn, closing(conn.cursor()) as cur, closing(conn.cursor()) as cur2:
        cur: MySQLCursor
        cur2: MySQLCursor
        logging.info("Preparing tweet_stats_i table.")
        cur.execute("DROP TABLE IF EXISTS tweet_stats_i")
        create_stmt = "CREATE TABLE tweet_stats_i (tweet_id BIGINT UNSIGNED,"
        for col in int_cols:
            create_stmt += f"{col} INTEGER UNSIGNED, ur_{col} INTEGER UNSIGNED,"
        for col in float_cols:
            create_stmt += f"{col} FLOAT UNSIGNED, ur_{col} FLOAT UNSIGNED,"
        create_stmt += "PRIMARY KEY (tweet_id)) ENGINE=ARIA TRANSACTIONAL=0 PAGE_CHECKSUM=0"
        cur.execute(create_stmt)
        logging.info("Calculating statistics for singleton ur-conversations.")
        cur.execute("""
            INSERT IGNORE INTO tweet_stats_i
            SELECT tweet_id,
            0 AS children,
            0 AS ur_children,
            0 AS descendants,
            0 AS ur_descendants,
            0 AS leaf_descendants,
            0 AS ur_leaf_descendants,
            0 AS max_depth,
            0 AS ur_max_depth,
            1 AS t_authors,
            1 AS ur_t_authors,
            reply_count AS t_reply_count,
            reply_count AS ur_t_reply_count,
            quote_count AS t_quote_count,
            quote_count AS ur_t_quote_count,
            like_count AS t_like_count,
            like_count AS ur_t_like_count,
            retweet_count AS t_retweet_count,
            retweet_count AS ur_t_retweet_count,
            0 AS branching_factor,
            0 AS ur_branching_factor,
            0 AS mean_depth,
            0 AS ur_mean_depth,
            0 AS mad_depth,
            0 AS ur_mad_depth,
            reply_count AS mean_reply_count,
            reply_count AS ur_mean_reply_count,
            0 AS mad_reply_count,
            0 AS ur_mad_reply_count,
            quote_count AS mean_quote_count,
            quote_count AS ur_mean_quote_count,
            0 AS mad_quote_count,
            0 AS ur_mad_quote_count,
            like_count AS mean_like_count,
            like_count AS ur_mean_like_count,
            0 AS mad_like_count,
            0 AS ur_mad_like_count,
            retweet_count AS mean_retweet_count,
            retweet_count AS ur_mean_retweet_count,
            0 AS mad_retweet_count,
            0 AS ur_mad_retweet_count
            FROM tweets_i
            WHERE tweet_id IN (SELECT ur_conversation_id FROM tweets_i
            GROUP BY ur_conversation_id
            HAVING COUNT(*)=1)
            """)
        logging.info("Calculating stat data.")
        cur.execute("""
            SELECT ur_conversation_id FROM tweets_i
            GROUP BY ur_conversation_id
            HAVING COUNT(*)>1 
            """)
        for (ur_conversation_id,) in tqdm(cur.fetchall(), unit="ur-conversations"):
            cur.execute("SELECT tweet_id, author_id, in_reply_to, retweet_of, quotes, reply_count, quote_count, like_count, retweet_count FROM tweets_i WHERE ur_conversation_id=%s ORDER BY tweet_id DESC", (ur_conversation_id,))
            enrich_conversation(cur2, cur.fetchall())
        logging.info("Done.")


if __name__ == '__main__':
    enrich_conversations()
