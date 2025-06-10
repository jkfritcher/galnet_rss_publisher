import sys
import os
import json
import logging
import re
import time
from typing import Any

from botocore.exceptions import ClientError
import boto3
import boto3.session

import atoma, atoma.rss
import bleach
import requests

# Environment Variables
#
# LOCAL_STATE=<bool> -- Store read state locally in rss_state.json instead of in S3
# S3_BUCKET_NAME=<bucket name> -- S3 bucket name to store RSS state in
# S3_KEY_NAME=<object name> -- Name to give RSS state in S3
# RSS_URL=<RSS feed url> -- URL of the RSS feed to publish
# WEBHOOK_URL=<Webhook URL> -- URL of Webhook to publish to
# LOGGING_LEVEL=<log level> -- Level for logging messages

GALNET_BASE_URL = 'https://community.elitedangerous.com/en/galnet/uid'
MAX_MSG_LEN = 2000
MAX_ARTICLES_SEEN = 30
LOCAL_STATE_FILENAME = 'rss_state.json'
REQUEST_TIMEOUT = 5
EMDASH = '\u2014'

# Set up root log level
logger = logging.getLogger()
logger.setLevel(logging.INFO)
# Get our named logger
logger = logging.getLogger(__name__)

# Set local testing flag
local_state = os.environ.get('LOCAL_STATE', 'False').lower() == 'true'

# Get what region we're running in
region = os.environ.get('AWS_REGION')

# Create boto session and clients
aws = boto3.session.Session(region_name=region)
secretsmanager = aws.client('secretsmanager')
if not local_state:
    s3 = aws.client('s3')

br_pat = re.compile('<br ?/>')

s3_bucket = os.environ.get('S3_BUCKET_NAME')
s3_key = os.environ.get('S3_KEY_NAME')
rss_url = os.environ['RSS_URL']

log_levels = {
    'debug':    logging.DEBUG,
    'info':     logging.INFO,
    'warning':  logging.WARNING,
    'error':    logging.ERROR,
    'critical': logging.CRITICAL
}
def set_logger_level(level: str|None):
    if level:
        level_num = log_levels.get(level.lower())
        if level_num:
            # Set level at root log
            logger.setLevel(level_num)
        else:
            logger.warning(f"Invalid logging level specified, ignorning. '{level}'")

def paginate_message(content: str) -> list[str]:
    parts = []
    while(len(content) > 0):
        if len(content) <= MAX_MSG_LEN:
            parts.append(content)
            break

        idx = content.rfind("\n\n", 0, MAX_MSG_LEN - 3)
        if idx < 0:
            logger.warning("Paragraph break was not found! Trying to break at a word.")
            idx = content.rfind(" ", 0, MAX_MSG_LEN - 3)
            if idx < 0:
                logger.warning("Word break not found, breaking arbitrarily.")
                idx = MAX_MSG_LEN - 3

        parts.append(content[:idx] + "```")
        if len(content) > idx:
            if content[idx] == "\n":
                idx += 2
            elif content[idx] == " ":
                idx += 1
            content = "```\n" + content[idx:]
        else:
            content = ''

    return parts

# Check items for things we wish to filter out
def filter_item(title: str, body: str) -> bool:
    if title == "Week in Review":
        return True
    return False

def get_webhook_url() -> str:
    webhook_url = os.environ['WEBHOOK_URL']
    if not webhook_url:
        raise ValueError("WEBHOOK_URL is not defined!")
    if not webhook_url.startswith('arn') and not webhook_url.startswith('http'):
        logger.error(f"Invalid webhook URL provided, '{webhook_url}'")
        raise ValueError("WEBHOOK_URL must be an arn or http/s URL")

    if webhook_url.startswith('arn:'):
        logger.debug(f"Found ARN for URL, {webhook_url}")
        service_name = webhook_url.split(':', maxsplit=5)[2]
        if service_name == 'secretsmanager':
            logger.info("Fetching webhook URL from Secrets Manager")
            res = secretsmanager.get_secret_value(SecretId=webhook_url)
            return res['SecretString']
        else:
            logger.error("Unknown ARN service specified")
            raise NotImplementedError

    if webhook_url.startswith('http'):
        logger.debug("Found HTTP/S URL")
        return webhook_url

    raise RuntimeError("This error shouldn't be reached")

def load_state() -> dict[str, Any]:
    if not local_state:
        try:
            res = s3.get_object(Bucket=s3_bucket, Key=s3_key)
            state_str = res['Body'].read().decode('utf-8')
        except ClientError as ex:
            if ex.response['Error']['Code'] != 'NoSuchKey':
                logger.error(f"S3 error occurred: {ex}")
                raise ex
            logger.info("No previous state found, starting fresh")
            state_str = '{}'
    else:
        try:
            with open(LOCAL_STATE_FILENAME, 'r') as f:
                state_str = f.read()
        except FileNotFoundError:
            logger.info("No previous state found, starting fresh")
            state_str = '{}'

    state = json.loads(state_str)
    logger.info("Last saved state loaded")
    logger.debug(f"Last Saved State - {state}")
    return state

def save_state(state: dict[str, Any]):
    state_str = json.dumps(state, indent=4)
    logger.debug(f"Updated save state: {state_str}")
    if not local_state:
        res = s3.put_object(Bucket=s3_bucket, Key=s3_key, Body=state_str.encode('utf-8'))
    else:
        with open(LOCAL_STATE_FILENAME, 'w') as f:
            f.write(state_str)
    logger.info("Wrote updated save state")

def fetch_rss_feed(url: str) -> atoma.rss.RSSChannel|None:
    try:
        res = requests.get(url, timeout=REQUEST_TIMEOUT)
        res.raise_for_status()
        feed = atoma.parse_rss_bytes(res.content)
        logger.info("RSS feed fetched and parsed successfully")
        return feed
    except requests.RequestException as ex:
        logger.error(f"Failed to fetch RSS feed: {ex}")
        return None

def process_feed_items(items: list[Any], articles_seen: list[str]) -> list[tuple[str, str]]:
    articles_to_publish: list[tuple[str, str]] = []
    for item in items:
        guid = bleach.clean(item.guid)
        if guid in articles_seen:
            logger.debug(f"Article {guid} has been seen already, skipping")
            continue

        # Sanitize article data
        title = bleach.clean(item.title)
        desc = re.sub(br_pat, '\n', item.description).rstrip()
        desc = bleach.clean(desc)

        # Ignore things we're not interested in
        if filter_item(title, desc):
            continue

        if not articles_to_publish:
            logger.info("New articles found:")

        logger.info(f"{guid} {EMDASH} {title}")

        # Build and publish article
        content = f"**{title}** {EMDASH} [Link]({GALNET_BASE_URL}/{guid})\n```\n{desc}```"
        articles_to_publish.append((guid, content))
    return articles_to_publish

def publish_articles(webhook_url: str, articles_to_publish: list[tuple[str, str]], articles_seen: list[str]) -> int:
    logger.info("Publishing new articles...")
    published_count: int = 0
    for (guid, content) in articles_to_publish:
        logger.debug(f"Article Length - {len(content)}")
        parts = paginate_message(content)
        for part in parts:
            content = { 'content': part }
            res = requests.post(webhook_url, json=content, timeout=REQUEST_TIMEOUT)
            logger.debug(f"{res} - {res.content}")
            time.sleep(1)
        articles_seen.append(guid)
        published_count += 1
        logger.info(f"Successfully published {guid}")
    return published_count

def prune_articles_seen(articles_seen: list[str], state: dict[str, Any]):
    num_articles_seen = len(articles_seen)
    if num_articles_seen > MAX_ARTICLES_SEEN:
        num_to_prune = num_articles_seen - MAX_ARTICLES_SEEN
        state['articles_seen'] = articles_seen[num_to_prune:]
        logger.debug(f"Pruned {num_to_prune} articles")

def lambda_handler(event: dict[str, Any]|None, context:Any|None):
    # Set logging level from the environment
    set_logger_level(os.environ.get('LOGGING_LEVEL'))

    logger.debug(f"event = {json.dumps(event)}")

    # Load previous state
    state = load_state()

    # Get list of articles already seen
    articles_seen: list[str] = state.setdefault('articles_seen', [])

    # Get / parse RSS feed
    feed = fetch_rss_feed(rss_url)
    if not feed:
        return {"statusCode": 500, "body": "Failed to fetch RSS feed"}

    # Process in chronological order
    items = feed.items
    items.reverse()   # Oldest to newest

    # Find articles to publish
    articles_to_publish = process_feed_items(items, articles_seen)
    if articles_to_publish:
        # Fetch webhook url
        webhook_url = get_webhook_url()

        # Publish articles
        published_count = publish_articles(webhook_url, articles_to_publish, articles_seen)

        # Prune seen articles
        prune_articles_seen(articles_seen, state)

        # Save updated state
        save_state(state)

        logger.info(f"Published {published_count} new article(s).")
    else:
        logger.info("No new articles found to publish")

if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout)
    lambda_handler(None, None)
