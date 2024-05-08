import os
import json
import logging
import re
from threading import local
import time

from base64 import b64decode

from botocore.exceptions import ClientError
import boto3

import atoma
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

# Set up root log level
logger = logging.getLogger()
logger.setLevel(logging.INFO)
# Get our named logger
logger = logging.getLogger(__name__)

# Set local testing flag
local_state = bool(os.environ.get('LOCAL_STATE', False))

# Get what region we're running in
region = os.environ.get('AWS_REGION')

# Create boto session and clients
aws = boto3.session.Session(region_name=region)
secretsmanager = aws.client('secretsmanager')
if not local_state:
    s3 = aws.client('s3')


GALNET_BASE_URL = 'https://community.elitedangerous.com/en/galnet/uid'

MAX_MSG_LEN = 2000

MAX_ARTICLES_SEEN = 30

br_pat = re.compile('<br ?/>')

s3_bucket = os.environ.get('S3_BUCKET_NAME')
s3_key = os.environ.get('S3_KEY_NAME')
rss_url = os.environ['RSS_URL']

log_levels = {
    "debug":    logging.DEBUG,
    "info":     logging.INFO,
    "warning":  logging.WARNING,
    "error":    logging.ERROR,
    "critical": logging.CRITICAL
}
def set_logger_level(level):
    if level:
        level = log_levels.get(level.lower())
        if level:
            # Set level at root log
            logger.setLevel(level)
        else:
            logger.warning("Invalid logging level specified, ignorning. '{}'".format(level))

def paginate_message(content):
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
def filter_item(title, body):
    if title == 'Week in Review':
        return True
    return False

def get_webhook_url():
    webhook_url = os.environ['WEBHOOK_URL']
    if webhook_url.startswith('arn:'):
        logger.debug('Found ARN for URL, {}'.format(webhook_url))
        service_name = webhook_url.split(':', maxsplit=5)[2]
        if service_name == 'secretsmanager':
            logger.info('Fetching webhook URL from Secrets Manager.')
            res = secretsmanager.get_secret_value(SecretId=webhook_url)
            webhook_url = res['SecretString']
            return webhook_url
        else:
            logger.error('Unknown ARN service specified.')
            raise NotImplementedError
        return None

    if webhook_url.startswith('https') or webhook_url.startswith('http'):
        logger.debug('Found HTTP/S URL')
        return webhook_url

    # Should not reach here
    raise RuntimeError

def lambda_handler(event, context):
    # Set logging level from the environment
    set_logger_level(os.environ.get("LOGGING_LEVEL"))

    logger.debug('event = {}'.format(json.dumps(event)))

    # Load previous state
    if not local_state:
        try:
            res = s3.get_object(Bucket=s3_bucket, Key=s3_key)
            state_str = res['Body'].read().decode('utf-8')
        except ClientError as ex:
            if ex.response['Error']['Code'] != 'NoSuchKey':
                raise ex
            state_str = '{}'
    else:
        with open('rss_state.json', 'r') as f:
            state_str = f.read()
    state = json.loads(state_str)
    logger.info('Last saved state loaded')
    logger.debug('Last Saved State - {}'.format(state))

    # Get list of articles already seen
    articles_seen = state.get('articles_seen')
    if not articles_seen:
        articles_seen = []
        state['articles_seen'] = articles_seen

    # Get / parse RSS feed
    res = requests.get(rss_url)
    if res.status_code != 200:
        logger.warn('Unexpected status code while fetching RSS feed, {}'.format(res.status_code))
        return
    feed = atoma.parse_rss_bytes(res.content)
    items = feed.items   # Newest to oldest
    items.reverse()   # Oldest to newest
    logger.info('RSS feed fetched and parsed successfully.')

    articles_to_publish = []
    for item in items:
        guid = bleach.clean(item.guid)
        if guid in articles_seen:
            logger.debug('Article {} has been seen already, skipping.'.format(guid))
            continue

        # Sanitize article data
        title = bleach.clean(item.title)
        desc = re.sub(br_pat, "\n", item.description).rstrip()
        desc = bleach.clean(desc)

        # Ignore things we're not interested in
        if filter_item(title, desc):
            continue

        if not articles_to_publish:
            logger.info('New articles found:')

        logger.info('{0} Title - {1}'.format(guid, title))

        # Build and publish article
        content = "**{0}** \u2014 [Link]({1}/{2})\n```\n{3}```".format(title, GALNET_BASE_URL, guid, desc)   # \u2014 - em-dash
        articles_to_publish.append((guid, content))

    if articles_to_publish:
        # Fetch webhook url
        webhook_url = get_webhook_url()

        # Paginate and publish articles
        logger.info('Publishing new articles...')
        for (guid, content) in articles_to_publish:
            logger.debug('Article Length - {}'.format(len(content)))
            parts = paginate_message(content)
            for part in parts:
                content = { 'content': part }
                res = requests.post(webhook_url, json=content)
                logger.debug("{0} - {1}".format(res, res.content))
                time.sleep(1)
            articles_seen.append(guid)
            logger.info('Successfully published {}.'.format(guid))

        # Prune seen articles to keep it reasonable
        num_articles_seen = len(articles_seen)
        if num_articles_seen > MAX_ARTICLES_SEEN:
            num_to_prune = num_articles_seen - MAX_ARTICLES_SEEN
            state['articles_seen'] = articles_seen[num_to_prune:]
            logger.debug('Pruned {} articles'.format(num_to_prune))

        # Save updated state
        state_str = json.dumps(state, indent=4)
        logger.debug('Updated save state: {}'.format(state_str))
        if not local_state:
            res = s3.put_object(Bucket=s3_bucket, Key=s3_key, Body=state_str.encode('utf-8'))
        else:
            with open('rss_state.json', 'w') as f:
                f.write(state_str)
        logger.info('Wrote updated save state.')
        logger.info('Published {} new article(s).'.format(len(articles_to_publish)))
    else:
        logger.info('No new articles found to publish.')

if __name__ == '__main__':
    logging.basicConfig(filename='/dev/stdout')
    lambda_handler(None, None)
