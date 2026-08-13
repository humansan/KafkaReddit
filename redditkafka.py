import praw
import kafka
from kafka import KafkaProducer
from bs4 import BeautifulSoup
import os
import sys
from dotenv import load_dotenv

if len(sys.argv) < 2:
        print("""
        Usage: redditkafka.py <bootstrap-server> <topic (optional)>
        """, file=sys.stderr)
        sys.exit(-1)

KAFKA_SERVER = sys.argv[1]
# localhost:9092
SUBREDDIT = "gaming"
TOPIC_NAME = "reddit"

#if len(sys.argv) >= 3:
#	TOPIC_NAME = sys.argv[2]

# Credentials come from a local .env file (see .env.example). Never commit .env.
load_dotenv()

CLIENT_ID = os.environ.get("REDDIT_CLIENT_ID")
CLIENT_SECRET = os.environ.get("REDDIT_CLIENT_SECRET")
USER_AGENT = os.environ.get("REDDIT_USER_AGENT", "linux:big_data_hw3_kafka")

if not CLIENT_ID or not CLIENT_SECRET:
	print("""
	Missing Reddit credentials.
	Copy .env.example to .env and fill in REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET.
	""", file=sys.stderr)
	sys.exit(-1)

reddit = praw.Reddit(
	client_id = CLIENT_ID,
	client_secret = CLIENT_SECRET,
	user_agent = USER_AGENT)

print(reddit.read_only)

def htmltotext(html_text):
	soup = BeautifulSoup(html_text, "html.parser")
	plain_text = soup.get_text()
	return plain_text

producer = KafkaProducer(bootstrap_servers = KAFKA_SERVER)

for comment in reddit.subreddit(SUBREDDIT).stream.comments():
	#print(comment.body)
	producer.send(TOPIC_NAME, htmltotext(str(comment.body_html)).encode('utf-8'))



