#!/usr/bin/env python3

import logging

import requests
from more_itertools import chunked
import json
from twarc.client2 import Twarc2
from tqdm import tqdm
from os.path import exists
import sys
import click

from twarc.decorators2 import catch_request_exceptions, rate_limit

twarc_log = logging.getLogger("twarc")


class MyTwarc2(Twarc2):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get(self, *args, **kwargs):
        """
        Make a GET request to a specified URL.

        Args:
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.

        Returns:
            requests.Response: Response from Twitter API.
        """
        if not self.client:
            self.connect()
        twarc_log.info("getting %s %s", args, kwargs)
        r = self.last_response = self.client.get(*args, timeout=(3.05, 31), **kwargs)
        return r
    get = catch_request_exceptions(get, tries=7)
    get = rate_limit(get, tries=7)


@click.command()
@click.option('-i', '--input', required=True, help="input file containing conversation ids, one per line")
@click.option('-o', '--output', required=True, help="output jsonl file which will contain all conversation tweets")
@click.option('-s', '--status', required=True, help="status file for recovering")
@click.option('-l', '--log', required=True, help="error log file")
@click.option('-t', '--bearer-token', required=True, help="Twitter academic bearer token to use")
def fetch_conversations(input: str, output: str, status: str, log: str, bearer_token: str):
    """Program to efficiently fetch full Twitter conversation threads given conversation ids"""
    with open(input, 'rt') as cf:
        queries = list(["conversation_id:" + " OR conversation_id:".join(conversation_ids) for conversation_ids in chunked([line.rstrip('\n') for line in cf.readlines()], 26)])
    if exists(status):
        with open(status, 'rt') as sf:
            parts = sf.readline().split('/')
            if len(parts) == 1 and parts[0] == 'done.\n':
                sys.exit("Status file reports crawl already complete.")
            elif len(parts) == 4:
                start = int(parts[0])
                next_token = parts[2] if parts[1] != "0" else None
                assert queries[start] == parts[3].rstrip('\n'), f"{queries[start]} != {parts[3]}"
                logging.warning(f"Continuing from page {parts[1]} of query {start}.")
            else:
                start = 0
                next_token = None
    else:
        start = 0
        next_token = None
    queries = queries[start:]
    t = MyTwarc2(bearer_token=bearer_token)
    with open(output, 'at') as of, open(status, 'at') as sf, open(log, 'at') as lf:
        for index, query in enumerate(tqdm(queries, unit="query", smoothing=0)):
            try:
                for page_index, result_page in enumerate(tqdm(t.search_all(query, tweet_fields="attachments,author_id,conversation_id,created_at,entities,geo,id,in_reply_to_user_id,lang,public_metrics,text,possibly_sensitive,referenced_tweets,reply_settings,source,withheld", max_results=500, next_token=next_token), leave=False, unit="page")):
                    json.dump(result_page, of)
                    of.write("\n")
                    of.flush()
                    sf.truncate(0)
                    if 'meta' in result_page and 'next_token' in result_page['meta']:
                        sf.write(f"{start+index}/{page_index}/{result_page['meta']['next_token']}/{query}")
                    elif index + 1 < len(queries):
                        sf.write(f"{start + index + 1}/0//{queries[index + 1]}")
                    else:
                        sf.write("done.")
                    sf.flush()
                next_token = None
            except requests.exceptions.HTTPError as exception:
                if exception.response.status_code == 403:
                    logging.exception(f"Got 403 Unauthorized for {exception.response.url}.")
                    raise
                logging.exception(
                    f"Too many request/connection exceptions processing {start + index}/{query}. Abandoning and moving on to the next one.")
                lf.write(f"{start + index}/{query}/{exception}\n")
                lf.flush()
            except (requests.exceptions.RequestException, ConnectionError) as exception:
                logging.exception(f"Too many request/connection exceptions processing {start+index}/{query}. Abandoning and moving on to the next one.")
                lf.write(f"{start+index}/{query}/{exception}\n")
                lf.flush()
            except Exception:
                logging.exception(f"Unknown fatal error processing {query}.")
                raise


if __name__ == '__main__':
    fetch_conversations()
        
