from __future__ import print_function

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, udf
from pyspark.sql.functions import split
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import struct, to_json
import spacy

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("""
        Usage: entitycount.py <bootstrap-server>\nOR\nUsage: entitycount.py <bootstrap-server> <input-topic> <output-topic>
        """, file=sys.stderr)
        sys.exit(-1)

    bootstrapServers = sys.argv[1]
    subscribeType = sys.argv[2]
    #topics = sys.argv[3]

    IN_TOPIC = "reddit"
    OUT_TOPIC = "entity-count"

    if len(sys.argv) >= 4:
        IN_TOPIC = sys.argv[3]
        OUT_TOPIC = sys.argv[4]

    spark = SparkSession\
        .builder\
        .appName("StructuredKafkaWordCount")\
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Create DataSet representing the stream of input lines from kafka
    lines = spark\
        .readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", bootstrapServers)\
        .option("subscribe", IN_TOPIC)\
	.option("startingOffsets", "earliest")\
        .load()\
        .selectExpr("CAST(value AS STRING)")
    
    nlp = spacy.load('en_core_web_sm')

    exclude_types = ["MONEY", "TIME", "QUANTITY", "ORDINAL", "CARDINAL"]

    def namedents(line):
        doc = nlp(line)
        return [x.text for x in doc.ents if x.label_ not in exclude_types]
    
    entUDF = udf(lambda x: namedents(x), ArrayType(StringType()))
	
    # Split the lines into words
    # explode turns each item in an array into a separate row
    entities = lines.select(explode(entUDF(lines.value)).alias('entity'))

    # Generate running word count
    counts = entities.groupBy('entity').count().orderBy("count", ascending=False).limit(20)
    kafkawrite = counts.selectExpr("CAST(entity AS STRING) AS key", "to_json(struct(entity, count)) AS value")

    # Start running the query that prints the running counts to the console
    query = kafkawrite\
        .writeStream\
        .outputMode('complete')\
        .format('kafka')\
	.option("kafka.bootstrap.servers", bootstrapServers)\
	.option("topic", OUT_TOPIC)\
	.option("checkpointLocation", "checkpoint")\
	.start()

    query.awaitTermination()

