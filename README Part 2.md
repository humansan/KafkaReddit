## Prerequisites:
Have python, spark, kafka, and elastic (elastic, logstash, kibana) installed.

Python libraries needed (install these in the python environment you would like to run the python files):
- praw
- kafka-python
- spacy

Development and testing was on WSL in a Ubuntu installation.

## 1. Start Kafka

Start Kafka Server. For me this was done by going to the folder where Kafka is installed and running the following in terminal:
```
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"

bin/kafka-storage.sh format --standalone -t $KAFKA_CLUSTER_ID -c config/server.properties

bin/kafka-server-start.sh config/server.properties
```

## 2. Create Kafka Topics

```
bin/kafka-topics.sh --create --topic reddit --bootstrap-server localhost:9092
bin/kafka-topics.sh --create --topic entity-count --bootstrap-server localhost:9092
```


## Python Environment (may be optional)

Enter python environment where the required python packages are installed. If packages are installed globally this is not needed. For me this was done using:
```
source pyenv/bin/activate
```

Make sure you have these packages:
- praw
- kafka-python
- spacy
- python-dotenv

Install them all with:
```
pip install -r requirements.txt
python -m spacy download en_core_web_sm
```

## Reddit API Credentials

`redditkafka.py` reads its Reddit credentials from environment variables, loaded from a local
`.env` file. Set this up before running it:

1. Create a "script" type app at https://www.reddit.com/prefs/apps to get a client ID and secret.
2. Copy the template and fill in your values:
```
cp .env.example .env
```
3. Edit `.env`:
```
REDDIT_CLIENT_ID=your_client_id_here
REDDIT_CLIENT_SECRET=your_client_secret_here
REDDIT_USER_AGENT=linux:big_data_hw3_kafka
```

`.env` is listed in `.gitignore` and must never be committed. The script exits with an error
message if either credential is missing.

## 3. kafkareddit.py

`kafkareddit.py` gets comments from r/gaming, and sends it to a Kafka topic.

Run `redditkafka.py` using the following format:
```
Usage: redditkafka.py <bootstrap-server> <topic (optional)>
```

I did `python3 redditkafka.py localhost:9092`

The default topic is `reddit`. If you would like to save to a different topic, add it as an argument

## 4. entitycount.py

To run `entitycount.py`, the python module spacy must be available to all spark clusters that will run the job. For me, this was done using venv-pack like so:

1. Install venv-pack with `pip install venv-pack` if not available
2. `venv-pack -o pyspark_venv.tar.gz`
3. In command line where `entitycount.py` is run from:
```
export PYSPARK_PYTHON=./pyenv/bin/python
```


Run entitycount.py with the following format:
```
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 entitycount.py localhost:9092
OR
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 entitycount.py localhost:9092 TOPIC1 TOPIC2
```

The default topics are `reddit` and `entity-count`. Replace TOPIC1 and TOPIC2 with custom topics if needed.

Replace `localhost:9092` if Kafka server is located elsewhere.

NOTE: To send Spark an archive of python packages add --archives like so:
```
spark-submit --archives pyspark_venv.tar.gz#environment --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 entitycount.py localhost:9092
```

## 5. Elastic, Logstash, Kibana (for Visualization, optional)

Make sure you have Elasticsearch, Kibana, and Logstash installed.

Create Logstash pipeline configuration and safe it to Logstash's config folder. My config file is included in this project folder as `logstash-config.conf` and its code is shown below:

```
input {
  kafka {
    bootstrap_servers => "localhost:9092" # <- Change this if kafka server is different
    topics => ["entity-count"] # <- Change this to TOPIC2 if needed
    codec => "json"
    group_id => "logstash-entity-count"
  }
}
output {
  elasticsearch {
    hosts => ["http://localhost:9200"] # <- Change this if Change this if elasticsearch server is different
    index => "entity-counts-%{+YYYY.MM.dd}"
  }
}

```

Start elasticsearch, logstash, and kibana.

Go to Kibana. For me Kibana by default launches at `localhost:5601`.

For visualization, click on the following:
- Sidebar Menu > Analytics > Visualize Library > Create new Visualization > Lens
- In the Visualization creator, drag and drop fields as needed.

# Part 2: Graph Algorithms on LastFM Social Media Data

-	CS 6350 Assignment 3.ipynb is in the zip file
-	Outputs of all queries are included in the Python notebook

You can also find the databricks notebook here:
-	https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/273883669774366/4266033147520283/82784278503376/latest.html

Data is from SNAP:
-	LastFM Asia Social Network
-	https://snap.stanford.edu/data/lastfm_asia.zip

The zip file contains 2 csv files:
1.	Lastfm_asia_edges.csv = contains edges
2.	Lastfm_asia_target.csv = contains nodes

To run the notebook, update the path to these 2 files in the variables ‘edge_csv_path’ and ‘vert_csv_path’ in the python notebook. Then run the notebook.