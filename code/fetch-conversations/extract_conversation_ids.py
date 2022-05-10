#%%
import json
import logging
from collections import Counter
from json import JSONDecodeError

logging.basicConfig(level = logging.INFO)

convs = Counter()

lines = 0
tweets = 0
treplies = 0
zreplies = 0

with open("tweets.jsonl", 'rt') as jsonlf:
    for jsonl in jsonlf:
        lines += 1
        try:
            d = json.loads(jsonl)
            for tweet in d['data']:
                replies = tweet['public_metrics']['reply_count']
                tweets += 1
                treplies += replies
                if replies == 0:
                    zreplies += 1
                else:
                    convs[tweet['conversation_id']] += replies
        except JSONDecodeError:
            logging.error(f"Error processing line {lines}")

#%%

with open("convoy_conversation_ids.txt", 'wt') as cf:
    for conv in convs.keys():
        cf.write(conv)
        cf.write('\n')


